# 6.2. batch_ingest-nyc_taxi-spark-iceberg
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
// spark is pre-bound by the Atlas Zeppelin interpreter (Spark Connect + lakehouse catalog)
```

**PySpark (Jupyter):**

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
```

Both setup implementations choose the scale from an explicit notebook override, then `DATASET_SCALE`, then `small`. They call the internal resolver once, validate its exact response and expected NYC Taxi object order, and retain one verified immutable generation as `datasetSparkUris` / `dataset_spark_uris`.

### 2.2 Read

**Scala (Zeppelin):**

```scala
val taxiPaths = datasetSparkUris
val raw = taxiPaths
  .map(path => spark.read.parquet(path).withColumn("passenger_count", col("passenger_count").cast("double")))
  .reduce(_.unionByName(_))
raw.printSchema()
```

**PySpark (Jupyter):**

```python
taxi_paths = dataset_spark_uris
raw = None
for path in taxi_paths:
    normalized = spark.read.parquet(path).withColumn('passenger_count', F.col('passenger_count').cast('double'))
    raw = normalized if raw is None else raw.unionByName(normalized)
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
