<!-- AUTO-GENERATED — do not edit; run scripts/build_docs.py -->
# Notebooks — batch_ingest-nyc_taxi-spark-iceberg
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
// spark is pre-bound by the Atlas Zeppelin interpreter (Spark Connect + lakehouse catalog)
```

**PySpark (Jupyter):**

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
```

### 2.2 Read

**Scala (Zeppelin):**

```scala
val raw = spark.read.parquet("s3a://landing/nyc_taxi/")
raw.printSchema()
```

**PySpark (Jupyter):**

```python
raw = spark.read.parquet("s3a://landing/nyc_taxi/")
raw.printSchema()
```

### 2.3 Transform

**Scala (Zeppelin):**

```scala
val bronze = raw
  .where($"tpep_pickup_datetime".isNotNull && ($"passenger_count" > 0))
  .withColumn("trip_date", to_date($"tpep_pickup_datetime"))
```

**PySpark (Jupyter):**

```python
bronze = (raw
  .where(F.col('tpep_pickup_datetime').isNotNull() & (F.col('passenger_count') > 0))
  .withColumn('trip_date', F.to_date('tpep_pickup_datetime')))
```

### 2.4 Write

**Scala (Zeppelin):**

```scala
bronze.writeTo("lakehouse.bronze.nyc_taxi_trips").using("iceberg").createOrReplace()
```

**PySpark (Jupyter):**

```python
bronze.writeTo("lakehouse.bronze.nyc_taxi_trips").using("iceberg").createOrReplace()
```

### 2.5 Verify

**Scala (Zeppelin):**

```scala
spark.table("lakehouse.bronze.nyc_taxi_trips").count()
```

**PySpark (Jupyter):**

```python
spark.table("lakehouse.bronze.nyc_taxi_trips").count()
```

## 3. Scala / PySpark parity

Both notebooks share the same numbered sections and produce identical Iceberg tables; only the language and interpreter differ.

## 4. How to run

Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or `jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.
