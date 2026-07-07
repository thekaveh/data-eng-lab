<!-- AUTO-GENERATED — do not edit; run scripts/build_docs.py -->
# Notebooks — scd2-online_retail-spark-iceberg
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
# lakehouse catalog pre-configured
```

## 2. Setup

### 3. Read

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.gold.dim_customer_scd2 (customer_id string, segment string, effective_from timestamp, effective_to timestamp, is_current boolean) USING iceberg").show(truncate=False)
spark.sql("INSERT INTO lakehouse.gold.dim_customer_scd2 VALUES ('C1','standard', TIMESTAMP '2023-01-01 00:00:00', NULL, true)").show(truncate=False)
spark.sql("SELECT customer_id, segment, is_current FROM lakehouse.gold.dim_customer_scd2 ORDER BY effective_from").show(truncate=False)
```

## 3. Read

### 4. Transform

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
spark.sql("UPDATE lakehouse.gold.dim_customer_scd2 SET effective_to = current_timestamp(), is_current = false WHERE customer_id = 'C1' AND is_current = true").show(truncate=False)
```

## 4. Transform

### 5. Write

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
spark.sql("INSERT INTO lakehouse.gold.dim_customer_scd2 VALUES ('C1','premium', current_timestamp(), NULL, true)").show(truncate=False)
```

## 5. Write

### 6. Verify

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
spark.sql("SELECT customer_id, segment, effective_to IS NOT NULL AS closed, is_current FROM lakehouse.gold.dim_customer_scd2 ORDER BY effective_from").show(truncate=False)
```

## 6. Verify

## 4. Scala / PySpark parity

Both notebooks share the same numbered sections and produce identical Iceberg tables; only the language and interpreter differ.

## 5. How to run

Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or `jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.
