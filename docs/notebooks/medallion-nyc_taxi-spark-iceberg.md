# Notebooks — medallion-nyc_taxi-spark-iceberg
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
from pyspark.sql import functions as F

spark = SparkSession.builder.remote("sc://spark-connect:15002").getOrCreate()
```

### 3. Read

**Scala (Zeppelin):**

```scala
val bronze = spark.table("lakehouse.bronze.nyc_taxi_trips")
```

**PySpark (Jupyter):**

```python
bronze = spark.table("lakehouse.bronze.nyc_taxi_trips")
```

### 4. Transform

**Scala (Zeppelin):**

```scala
val silver = bronze.dropDuplicates("tpep_pickup_datetime", "tpep_dropoff_datetime")
val gold = silver.groupBy($"trip_date")
  .agg(count("*").as("trips"), avg($"fare_amount").as("avg_fare"))
```

**PySpark (Jupyter):**

```python
silver = bronze.dropDuplicates(["tpep_pickup_datetime", "tpep_dropoff_datetime"])
gold = (silver.groupBy("trip_date")
        .agg(F.count("*").alias("trips"), F.avg("fare_amount").alias("avg_fare")))
```

### 5. Write

**Scala (Zeppelin):**

```scala
silver.writeTo("lakehouse.silver.nyc_taxi_trips").using("iceberg").createOrReplace()
gold.writeTo("lakehouse.gold.nyc_taxi_daily").using("iceberg").createOrReplace()
```

**PySpark (Jupyter):**

```python
silver.writeTo("lakehouse.silver.nyc_taxi_trips").using("iceberg").createOrReplace()
gold.writeTo("lakehouse.gold.nyc_taxi_daily").using("iceberg").createOrReplace()
```

### 6. Verify

**Scala (Zeppelin):**

```scala
spark.table("lakehouse.gold.nyc_taxi_daily").orderBy($"trip_date").show()
```

**PySpark (Jupyter):**

```python
spark.table("lakehouse.gold.nyc_taxi_daily").orderBy("trip_date").show()
```

## 3. Scala / PySpark parity

Both notebooks share the same numbered sections and produce identical Iceberg tables; only the language and interpreter differ.

## 4. How to run

Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or `jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.
