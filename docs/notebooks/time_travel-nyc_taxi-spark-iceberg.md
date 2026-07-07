# Notebooks — time_travel-nyc_taxi-spark-iceberg
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

spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
```

## 2. Setup

### 3. Read

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.silver.nyc_taxi_tt AS SELECT * FROM lakehouse.bronze.nyc_taxi_trips").show(truncate=False)
```

## 3. Read

### 4. Transform

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
spark.sql("INSERT INTO lakehouse.silver.nyc_taxi_tt SELECT * FROM lakehouse.bronze.nyc_taxi_trips WHERE passenger_count > 3").show(truncate=False)
```

## 4. Transform

### 5. Write

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
spark.sql("ALTER TABLE lakehouse.silver.nyc_taxi_tt CREATE BRANCH IF NOT EXISTS audit").show(truncate=False)
```

## 5. Write

### 6. Verify

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
spark.sql("SELECT committed_at, snapshot_id FROM lakehouse.silver.nyc_taxi_tt.history ORDER BY committed_at").show(truncate=False)
# time-travel: spark.sql("SELECT count(*) FROM lakehouse.silver.nyc_taxi_tt VERSION AS OF <snapshot_id>").show()
# rollback:    spark.sql("CALL lakehouse.system.rollback_to_snapshot('lakehouse.silver.nyc_taxi_tt', <snapshot_id>)").show()
```

## 6. Verify

## 4. Scala / PySpark parity

Both notebooks share the same numbered sections and produce identical Iceberg tables; only the language and interpreter differ.

## 5. How to run

Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or `jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.
