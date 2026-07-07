# Notebooks — incremental_upsert-online_retail-spark-iceberg
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
# lakehouse catalog pre-configured
```

### 3. Read

**Scala (Zeppelin):**

```scala
spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.silver.online_retail (invoice string, stock_code string, quantity int, price double) USING iceberg").show(false)
spark.sql("INSERT INTO lakehouse.silver.online_retail VALUES ('A1','SKU1',5,2.0), ('A2','SKU2',3,4.0)").show(false)
spark.sql("SELECT * FROM lakehouse.silver.online_retail ORDER BY invoice").show(false)
```

**PySpark (Jupyter):**

```python
spark.sql("CREATE TABLE IF NOT EXISTS lakehouse.silver.online_retail (invoice string, stock_code string, quantity int, price double) USING iceberg").show(truncate=False)
spark.sql("INSERT INTO lakehouse.silver.online_retail VALUES ('A1','SKU1',5,2.0), ('A2','SKU2',3,4.0)").show(truncate=False)
spark.sql("SELECT * FROM lakehouse.silver.online_retail ORDER BY invoice").show(truncate=False)
```

### 4. Transform

**Scala (Zeppelin):**

```scala
spark.sql("CREATE OR REPLACE TEMP VIEW online_retail_updates AS SELECT * FROM VALUES ('A1','SKU1',10,2.0), ('A3','SKU3',1,9.0) AS t(invoice, stock_code, quantity, price)").show(false)
```

**PySpark (Jupyter):**

```python
spark.sql("CREATE OR REPLACE TEMP VIEW online_retail_updates AS SELECT * FROM VALUES ('A1','SKU1',10,2.0), ('A3','SKU3',1,9.0) AS t(invoice, stock_code, quantity, price)").show(truncate=False)
```

### 5. Write

**Scala (Zeppelin):**

```scala
spark.sql("MERGE INTO lakehouse.silver.online_retail t USING online_retail_updates s ON t.invoice = s.invoice AND t.stock_code = s.stock_code WHEN MATCHED THEN UPDATE SET t.quantity = s.quantity WHEN NOT MATCHED THEN INSERT *").show(false)
```

**PySpark (Jupyter):**

```python
spark.sql("MERGE INTO lakehouse.silver.online_retail t USING online_retail_updates s ON t.invoice = s.invoice AND t.stock_code = s.stock_code WHEN MATCHED THEN UPDATE SET t.quantity = s.quantity WHEN NOT MATCHED THEN INSERT *").show(truncate=False)
```

### 6. Verify

**Scala (Zeppelin):**

```scala
spark.sql("SELECT * FROM lakehouse.silver.online_retail ORDER BY invoice").show(false)
// Re-running the MERGE is idempotent: same result.
```

**PySpark (Jupyter):**

```python
spark.sql("SELECT * FROM lakehouse.silver.online_retail ORDER BY invoice").show(truncate=False)
print("Re-running the MERGE is idempotent: same result.")
```

## 3. Scala / PySpark parity

Both notebooks share the same numbered sections and produce identical Iceberg tables; only the language and interpreter differ.

## 4. How to run

Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or `jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.
