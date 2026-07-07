# Notebooks — table_maintenance-nyc_taxi-spark-iceberg
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
// spark pre-bound (Spark Connect + lakehouse catalog)
```

**PySpark (Jupyter):**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
```

### 3. Read

**Scala (Zeppelin):**

```scala
spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.silver.nyc_taxi_tm AS SELECT * FROM lakehouse.bronze.nyc_taxi_trips").show(false)
spark.sql("INSERT INTO lakehouse.silver.nyc_taxi_tm SELECT * FROM lakehouse.bronze.nyc_taxi_trips WHERE passenger_count > 3").show(false)
spark.sql("SELECT count(*) AS files FROM lakehouse.silver.nyc_taxi_tm.files").show(false)
```

**PySpark (Jupyter):**

```python
spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.silver.nyc_taxi_tm AS SELECT * FROM lakehouse.bronze.nyc_taxi_trips").show(truncate=False)
spark.sql("INSERT INTO lakehouse.silver.nyc_taxi_tm SELECT * FROM lakehouse.bronze.nyc_taxi_trips WHERE passenger_count > 3").show(truncate=False)
spark.sql("SELECT count(*) AS files FROM lakehouse.silver.nyc_taxi_tm.files").show(truncate=False)
```

### 4. Transform

**Scala (Zeppelin):**

```scala
spark.sql("CALL lakehouse.system.rewrite_data_files(table => 'lakehouse.silver.nyc_taxi_tm', options => map('target-file-size-bytes','134217728'))").show(false)
```

**PySpark (Jupyter):**

```python
spark.sql("CALL lakehouse.system.rewrite_data_files(table => 'lakehouse.silver.nyc_taxi_tm', options => map('target-file-size-bytes','134217728'))").show(truncate=False)
```

### 5. Write

**Scala (Zeppelin):**

```scala
spark.sql("CALL lakehouse.system.expire_snapshots(table => 'lakehouse.silver.nyc_taxi_tm', older_than => current_timestamp(), retain_last => 1)").show(false)
spark.sql("CALL lakehouse.system.remove_orphan_files(table => 'lakehouse.silver.nyc_taxi_tm')").show(false)
```

**PySpark (Jupyter):**

```python
spark.sql("CALL lakehouse.system.expire_snapshots(table => 'lakehouse.silver.nyc_taxi_tm', older_than => current_timestamp(), retain_last => 1)").show(truncate=False)
spark.sql("CALL lakehouse.system.remove_orphan_files(table => 'lakehouse.silver.nyc_taxi_tm')").show(truncate=False)
```

### 6. Verify

**Scala (Zeppelin):**

```scala
spark.sql("SELECT count(*) AS snapshots FROM lakehouse.silver.nyc_taxi_tm.snapshots").show(false)
spark.sql("SELECT count(*) AS files FROM lakehouse.silver.nyc_taxi_tm.files").show(false)
```

**PySpark (Jupyter):**

```python
spark.sql("SELECT count(*) AS snapshots FROM lakehouse.silver.nyc_taxi_tm.snapshots").show(truncate=False)
spark.sql("SELECT count(*) AS files FROM lakehouse.silver.nyc_taxi_tm.files").show(truncate=False)
```

## 3. Scala / PySpark parity

Both notebooks share the same numbered sections and produce identical Iceberg tables; only the language and interpreter differ.

## 4. How to run

Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or `jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.
