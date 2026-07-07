# Notebooks — streaming_windows-events-spark-iceberg
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
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, TimestampType

spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
```

## 2. Setup

### 3. Read

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
schema = StructType().add("user_id", StringType()).add("event", StringType()).add("ts", TimestampType())
raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers","redpanda:9092").option("subscribe","events").option("startingOffsets","earliest").load()
events = raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e")).select("e.*")
```

## 3. Read

### 4. Transform

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
windows = events.withWatermark("ts", "10 minutes").groupBy(F.window("ts", "5 minutes"), F.col("event")).count()
```

## 4. Transform

### 5. Write

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
query = windows.writeStream.format("iceberg").outputMode("append").option("checkpointLocation", "s3a://checkpoints/event_windows").toTable("lakehouse.gold.event_windows")
# query.awaitTermination()
```

## 5. Write

### 6. Verify

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
spark.table("lakehouse.gold.event_windows").orderBy("window").show(truncate=False)
```

## 6. Verify

## 4. Scala / PySpark parity

Both notebooks share the same numbered sections and produce identical Iceberg tables; only the language and interpreter differ.

## 5. How to run

Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or `jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.
