<!-- AUTO-GENERATED — do not edit; run scripts/build_docs.py -->
# Notebooks — data_quality-nyc_taxi-spark-iceberg
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
```

**PySpark (Jupyter):**

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
```

### 2.2 Read

**Scala (Zeppelin):**

```scala
val df = spark.table("lakehouse.bronze.nyc_taxi_trips")
```

**PySpark (Jupyter):**

```python
df = spark.table("lakehouse.bronze.nyc_taxi_trips")
```

### 2.3 Transform

**Scala (Zeppelin):**

```scala
val rule = "fare_amount > 0 AND passenger_count BETWEEN 1 AND 6"
val valid = df.where(rule)
val quarantine = df.where(s"NOT ($rule) OR fare_amount IS NULL")
```

**PySpark (Jupyter):**

```python
rule = "fare_amount > 0 AND passenger_count BETWEEN 1 AND 6"
valid = df.where(rule)
quarantine = df.where(f"NOT ({rule}) OR fare_amount IS NULL")
```

### 2.4 Write

**Scala (Zeppelin):**

```scala
valid.writeTo("lakehouse.silver.nyc_taxi_clean").using("iceberg").createOrReplace()
quarantine.writeTo("lakehouse.silver.nyc_taxi_quarantine").using("iceberg").createOrReplace()
```

**PySpark (Jupyter):**

```python
valid.writeTo("lakehouse.silver.nyc_taxi_clean").using("iceberg").createOrReplace()
quarantine.writeTo("lakehouse.silver.nyc_taxi_quarantine").using("iceberg").createOrReplace()
```

### 2.5 Verify

**Scala (Zeppelin):**

```scala
spark.sql("SELECT (SELECT count(*) FROM lakehouse.silver.nyc_taxi_clean) AS clean, (SELECT count(*) FROM lakehouse.silver.nyc_taxi_quarantine) AS quarantined").show(false)
```

**PySpark (Jupyter):**

```python
spark.sql("SELECT (SELECT count(*) FROM lakehouse.silver.nyc_taxi_clean) AS clean, (SELECT count(*) FROM lakehouse.silver.nyc_taxi_quarantine) AS quarantined").show(truncate=False)
```

## 3. Scala / PySpark parity

Both notebooks share the same numbered sections and produce identical Iceberg tables; only the language and interpreter differ.

## 4. How to run

Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or `jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.
