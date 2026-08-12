# 6.5. streaming_ingest-gh_archive-spark-iceberg
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

Both setup implementations choose an explicit notebook scale override before `DATASET_SCALE`, default to `small`, and call the internal resolver once. They validate and retain one verified immutable generation plus its publication and manifest identities.

### 2.2 Read

**Scala (Zeppelin):**

```scala
val schema = new StructType().add("id", StringType).add("type", StringType).add("created_at", StringType)
val streams = datasetSparkUris.map(uri => spark.readStream.schema(schema).json(uri))
val stream = streams.reduce(_.unionByName(_))
```

**PySpark (Jupyter):**

```python
schema = StructType().add("id", StringType()).add("type", StringType()).add("created_at", StringType())
streams = [spark.readStream.schema(schema).json(uri) for uri in dataset_spark_uris]
stream = streams[0]
for next_stream in streams[1:]:
    stream = stream.unionByName(next_stream)
```

### 2.3 Transform

**Scala (Zeppelin):**

```scala
val events = stream.withColumn("created_at", $"created_at".cast("timestamp"))
```

**PySpark (Jupyter):**

```python
events = stream.withColumn("created_at", F.col("created_at").cast("timestamp"))
```

### 2.4 Write

**Scala (Zeppelin):**

```scala
val query = events.writeStream.format("iceberg").outputMode("append").option("checkpointLocation", s"s3a://checkpoints/gh_events_file/$datasetScale/$publicationId/$manifestSha256").toTable("lakehouse.bronze.gh_events_stream")
// query.awaitTermination() to keep stream running
```

**PySpark (Jupyter):**

```python
query = events.writeStream.format("iceberg").outputMode("append").option("checkpointLocation", f"s3a://checkpoints/gh_events_file/{dataset_scale}/{publication_id}/{manifest_sha256}").toTable("lakehouse.bronze.gh_events_stream")
# query.awaitTermination() to keep stream running
```

### 2.5 Verify

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
