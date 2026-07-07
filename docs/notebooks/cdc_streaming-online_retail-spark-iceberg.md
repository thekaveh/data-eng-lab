# Notebooks — cdc_streaming-online_retail-spark-iceberg
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
import org.apache.spark.sql.types._
import org.apache.spark.sql.DataFrame
```

**PySpark (Jupyter):**

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType

spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
```

### 2.2 Read

**Scala (Zeppelin):**

```scala
spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.silver.online_retail_cdc (invoice string, stock_code string, quantity int, price double) USING iceberg")
val schema = new StructType().add("invoice", StringType).add("stock_code", StringType).add("quantity", IntegerType).add("price", DoubleType)
val raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "redpanda:9092").option("subscribe", "online_retail_cdc").option("startingOffsets", "earliest").load()
val cdc = raw.select(from_json($"value".cast("string"), schema).as("c")).select("c.*")
```

**PySpark (Jupyter):**

```python
spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.silver.online_retail_cdc (invoice string, stock_code string, quantity int, price double) USING iceberg")
schema = StructType().add("invoice", StringType()).add("stock_code", StringType()).add("quantity", IntegerType()).add("price", DoubleType())
raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "redpanda:9092").option("subscribe", "online_retail_cdc").option("startingOffsets", "earliest").load()
cdc = raw.select(F.from_json(F.col("value").cast("string"), schema).alias("c")).select("c.*")
```

### 2.3 Transform

**Scala (Zeppelin):**

```scala
val parsed = cdc
// CDC rows are upserted per micro-batch in the Write step
```

**PySpark (Jupyter):**

```python
parsed = cdc
# CDC rows are upserted per micro-batch in the Write step
```

### 2.4 Write

**Scala (Zeppelin):**

```scala
val query = parsed.writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) =>
  batchDF.createOrReplaceTempView("cdc_batch")
  batchDF.sparkSession.sql("MERGE INTO lakehouse.silver.online_retail_cdc t USING cdc_batch s ON t.invoice = s.invoice AND t.stock_code = s.stock_code WHEN MATCHED THEN UPDATE SET t.quantity = s.quantity, t.price = s.price WHEN NOT MATCHED THEN INSERT *")
}.option("checkpointLocation", "s3a://checkpoints/online_retail_cdc").start()
```

**PySpark (Jupyter):**

```python
def upsert_batch(batch_df, batch_id):
    batch_df.createOrReplaceTempView("cdc_batch")
    batch_df.sparkSession.sql("MERGE INTO lakehouse.silver.online_retail_cdc t USING cdc_batch s ON t.invoice = s.invoice AND t.stock_code = s.stock_code WHEN MATCHED THEN UPDATE SET t.quantity = s.quantity, t.price = s.price WHEN NOT MATCHED THEN INSERT *")

query = parsed.writeStream.foreachBatch(upsert_batch).option("checkpointLocation", "s3a://checkpoints/online_retail_cdc").start()
```

### 2.5 Verify

**Scala (Zeppelin):**

```scala
spark.table("lakehouse.silver.online_retail_cdc").orderBy("invoice").show(false)
```

**PySpark (Jupyter):**

```python
spark.table("lakehouse.silver.online_retail_cdc").orderBy("invoice").show(truncate=False)
```

## 3. Scala / PySpark parity

Both notebooks share the same numbered sections and produce identical Iceberg tables; only the language and interpreter differ.

## 4. How to run

Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or `jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.
