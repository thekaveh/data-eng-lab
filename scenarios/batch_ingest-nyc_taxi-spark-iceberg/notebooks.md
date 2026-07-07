<!-- AUTO-GENERATED — do not edit; run scripts/build_docs.py -->
# Notebooks — batch_ingest-nyc_taxi-spark-iceberg
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

spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
```

## 2. Setup

### 3. Read

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
raw = spark.read.parquet("s3a://landing/nyc_taxi/")
raw.printSchema()
```

## 3. Read

### 4. Transform

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
bronze = (raw
  .where(F.col('tpep_pickup_datetime').isNotNull() & (F.col('passenger_count') > 0))
  .withColumn('trip_date', F.to_date('tpep_pickup_datetime')))
```

## 4. Transform

### 5. Write

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
bronze.writeTo("lakehouse.bronze.nyc_taxi_trips").using("iceberg").createOrReplace()
```

## 5. Write

### 6. Verify

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
spark.table("lakehouse.bronze.nyc_taxi_trips").count()
```

## 6. Verify

## 4. Scala / PySpark parity

Both notebooks share the same numbered sections and produce identical Iceberg tables; only the language and interpreter differ.

## 5. How to run

Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or `jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.
