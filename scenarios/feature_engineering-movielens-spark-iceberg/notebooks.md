<!-- AUTO-GENERATED — do not edit; run scripts/build_docs.py -->
# Notebooks — feature_engineering-movielens-spark-iceberg
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
ratings = spark.read.option("header", True).option("inferSchema", True).csv("s3a://landing/movielens/ratings.csv")
```

## 3. Read

### 4. Transform

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
userFeatures = ratings.groupBy("userId").agg(F.avg("rating").alias("avg_rating"), F.count("*").alias("num_ratings"))
movieFeatures = ratings.groupBy("movieId").agg(F.avg("rating").alias("movie_avg"), F.count("*").alias("popularity"))
```

## 4. Transform

### 5. Write

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
userFeatures.writeTo("lakehouse.gold.ml_user_features").using("iceberg").createOrReplace()
movieFeatures.writeTo("lakehouse.gold.ml_movie_features").using("iceberg").createOrReplace()
```

## 5. Write

### 6. Verify

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
spark.table("lakehouse.gold.ml_movie_features").orderBy(F.desc("popularity")).show(10, truncate=False)
```

## 6. Verify

## 4. Scala / PySpark parity

Both notebooks share the same numbered sections and produce identical Iceberg tables; only the language and interpreter differ.

## 5. How to run

Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or `jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.
