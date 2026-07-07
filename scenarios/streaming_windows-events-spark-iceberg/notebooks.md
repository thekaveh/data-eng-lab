<!-- AUTO-GENERATED — do not edit; run scripts/build_docs.py -->
# Notebooks — streaming_windows-events-spark-iceberg
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
```

**PySpark (Jupyter):**

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, TimestampType

spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
```

### 3. Read

**Scala (Zeppelin):**

```scala
val schema = new StructType().add("user_id", StringType).add("event", StringType).add("ts", TimestampType)
val raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "redpanda:9092").option("subscribe", "events").option("startingOffsets", "earliest").load()
val events = raw.select(from_json($"value".cast("string"), schema).as("e")).select("e.*")
```

**PySpark (Jupyter):**

```python
schema = StructType().add("user_id", StringType()).add("event", StringType()).add("ts", TimestampType())
raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers","redpanda:9092").option("subscribe","events").option("startingOffsets","earliest").load()
events = raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e")).select("e.*")
```

### 4. Transform

**Scala (Zeppelin):**

```scala
val windows = events.withWatermark("ts", "10 minutes").groupBy(window($"ts", "5 minutes"), $"event").count()
```

**PySpark (Jupyter):**

```python
windows = events.withWatermark("ts", "10 minutes").groupBy(F.window("ts", "5 minutes"), F.col("event")).count()
```

### 5. Write

**Scala (Zeppelin):**

```scala
val query = windows.writeStream.format("iceberg").outputMode("append").option("checkpointLocation", "s3a://checkpoints/event_windows").toTable("lakehouse.gold.event_windows")
// query.awaitTermination()
```

**PySpark (Jupyter):**

```python
query = windows.writeStream.format("iceberg").outputMode("append").option("checkpointLocation", "s3a://checkpoints/event_windows").toTable("lakehouse.gold.event_windows")
# query.awaitTermination()
```

### 6. Verify

**Scala (Zeppelin):**

```scala
spark.table("lakehouse.gold.event_windows").orderBy("window").show(false)
```

**PySpark (Jupyter):**

```python
spark.table("lakehouse.gold.event_windows").orderBy("window").show(truncate=False)
```

## 3. Scala / PySpark parity

Both notebooks share the same numbered sections and produce identical Iceberg tables; only the language and interpreter differ.

## 4. How to run

Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or `jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.
