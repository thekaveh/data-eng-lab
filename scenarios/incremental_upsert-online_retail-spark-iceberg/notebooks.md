<!-- AUTO-GENERATED — do not edit; run scripts/build_docs.py -->
# Notebooks — incremental_upsert-online_retail-spark-iceberg
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
spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.silver.online_retail (invoice string, stock_code string, quantity int, price double) USING iceberg").show(truncate=False)
spark.sql("INSERT INTO lakehouse.silver.online_retail VALUES ('A1','SKU1',5,2.0), ('A2','SKU2',3,4.0)").show(truncate=False)
spark.sql("SELECT * FROM lakehouse.silver.online_retail ORDER BY invoice").show(truncate=False)
```

## 3. Read

### 4. Transform

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
spark.sql("CREATE OR REPLACE TEMP VIEW online_retail_updates AS SELECT * FROM VALUES ('A1','SKU1',10,2.0), ('A3','SKU3',1,9.0) AS t(invoice, stock_code, quantity, price)").show(truncate=False)
```

## 4. Transform

### 5. Write

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
spark.sql("MERGE INTO lakehouse.silver.online_retail t USING online_retail_updates s ON t.invoice = s.invoice AND t.stock_code = s.stock_code WHEN MATCHED THEN UPDATE SET t.quantity = s.quantity WHEN NOT MATCHED THEN INSERT *").show(truncate=False)
```

## 5. Write

### 6. Verify

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
spark.sql("SELECT * FROM lakehouse.silver.online_retail ORDER BY invoice").show(truncate=False)
print("Re-running the MERGE is idempotent: same result.")
```

## 6. Verify

## 4. Scala / PySpark parity

Both notebooks share the same numbered sections and produce identical Iceberg tables; only the language and interpreter differ.

## 5. How to run

Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or `jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.
