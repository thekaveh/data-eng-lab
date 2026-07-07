<!-- AUTO-GENERATED — do not edit; run scripts/build_docs.py -->
# Notebooks — time_travel-nyc_taxi-spark-iceberg
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
// spark pre-bound (Spark Connect + lakehouse catalog)
```

**PySpark (Jupyter):**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
```

### 2.2 Read

**Scala (Zeppelin):**

```scala
spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.silver.nyc_taxi_tt AS SELECT * FROM lakehouse.bronze.nyc_taxi_trips").show(false)
```

**PySpark (Jupyter):**

```python
spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.silver.nyc_taxi_tt AS SELECT * FROM lakehouse.bronze.nyc_taxi_trips").show(truncate=False)
```

### 2.3 Transform

**Scala (Zeppelin):**

```scala
spark.sql("INSERT INTO lakehouse.silver.nyc_taxi_tt SELECT * FROM lakehouse.bronze.nyc_taxi_trips WHERE passenger_count > 3").show(false)
```

**PySpark (Jupyter):**

```python
spark.sql("INSERT INTO lakehouse.silver.nyc_taxi_tt SELECT * FROM lakehouse.bronze.nyc_taxi_trips WHERE passenger_count > 3").show(truncate=False)
```

### 2.4 Write

**Scala (Zeppelin):**

```scala
spark.sql("ALTER TABLE lakehouse.silver.nyc_taxi_tt CREATE BRANCH IF NOT EXISTS audit").show(false)
```

**PySpark (Jupyter):**

```python
spark.sql("ALTER TABLE lakehouse.silver.nyc_taxi_tt CREATE BRANCH IF NOT EXISTS audit").show(truncate=False)
```

### 2.5 Verify

**Scala (Zeppelin):**

```scala
spark.sql("SELECT committed_at, snapshot_id FROM lakehouse.silver.nyc_taxi_tt.history ORDER BY committed_at").show(false)
// time-travel: spark.sql("SELECT count(*) FROM lakehouse.silver.nyc_taxi_tt VERSION AS OF <snapshot_id>").show()
// rollback:    spark.sql("CALL lakehouse.system.rollback_to_snapshot('lakehouse.silver.nyc_taxi_tt', <snapshot_id>)").show()
```

**PySpark (Jupyter):**

```python
spark.sql("SELECT committed_at, snapshot_id FROM lakehouse.silver.nyc_taxi_tt.history ORDER BY committed_at").show(truncate=False)
# time-travel: spark.sql("SELECT count(*) FROM lakehouse.silver.nyc_taxi_tt VERSION AS OF <snapshot_id>").show()
# rollback:    spark.sql("CALL lakehouse.system.rollback_to_snapshot('lakehouse.silver.nyc_taxi_tt', <snapshot_id>)").show()
```

## 3. Scala / PySpark parity

Both notebooks share the same numbered sections and produce identical Iceberg tables; only the language and interpreter differ.

## 4. How to run

Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or `jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.
