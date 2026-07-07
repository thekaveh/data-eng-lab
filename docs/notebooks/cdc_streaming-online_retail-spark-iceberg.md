# Notebooks — cdc_streaming-online_retail-spark-iceberg
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
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType

spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
```

## 2. Setup

### 3. Read

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.silver.online_retail_cdc (invoice string, stock_code string, quantity int, price double) USING iceberg")
schema = StructType().add("invoice", StringType()).add("stock_code", StringType()).add("quantity", IntegerType()).add("price", DoubleType())
raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "redpanda:9092").option("subscribe", "online_retail_cdc").option("startingOffsets", "earliest").load()
cdc = raw.select(F.from_json(F.col("value").cast("string"), schema).alias("c")).select("c.*")
```

## 3. Read

### 4. Transform

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
parsed = cdc
# CDC rows are upserted per micro-batch in the Write step
```

## 4. Transform

### 5. Write

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
def upsert_batch(batch_df, batch_id):
    batch_df.createOrReplaceTempView("cdc_batch")
    batch_df.sparkSession.sql("MERGE INTO lakehouse.silver.online_retail_cdc t USING cdc_batch s ON t.invoice = s.invoice AND t.stock_code = s.stock_code WHEN MATCHED THEN UPDATE SET t.quantity = s.quantity, t.price = s.price WHEN NOT MATCHED THEN INSERT *")

query = parsed.writeStream.foreachBatch(upsert_batch).option("checkpointLocation", "s3a://checkpoints/online_retail_cdc").start()
```

## 5. Write

### 6. Verify

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
spark.table("lakehouse.silver.online_retail_cdc").orderBy("invoice").show(truncate=False)
```

## 6. Verify

## 4. Scala / PySpark parity

Both notebooks share the same numbered sections and produce identical Iceberg tables; only the language and interpreter differ.

## 5. How to run

Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or `jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.
