<!-- AUTO-GENERATED — do not edit; run scripts/build_docs.py -->
# Notebooks — federated_query-nyc_taxi-trino-iceberg
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
-- %trino is pre-bound to the Atlas Trino coordinator (catalog: lakehouse)
```

**PySpark (Jupyter):**

```python
from trino.dbapi import connect

cur = connect(host='trino', port=8080, user='atlas', catalog='lakehouse').cursor()
def q(sql):
    cur.execute(sql)
    return cur.fetchall()
```

### 2.2 Read

**Scala (Zeppelin):**

```scala
SELECT * FROM lakehouse.bronze.nyc_taxi_trips LIMIT 10
```

**PySpark (Jupyter):**

```python
q('SELECT * FROM lakehouse.bronze.nyc_taxi_trips LIMIT 10')
```

### 2.3 Transform

**Scala (Zeppelin):**

```scala
SELECT trip_date, count(*) AS trips, avg(fare_amount) AS avg_fare
FROM lakehouse.bronze.nyc_taxi_trips
GROUP BY trip_date ORDER BY trip_date
```

**PySpark (Jupyter):**

```python
q('SELECT trip_date, count(*) AS trips, avg(fare_amount) AS avg_fare '
  'FROM lakehouse.bronze.nyc_taxi_trips GROUP BY trip_date ORDER BY trip_date')
```

### 2.4 Write

**Scala (Zeppelin):**

```scala
CREATE TABLE IF NOT EXISTS lakehouse.gold.nyc_taxi_daily_trino AS
SELECT trip_date, count(*) AS trips, avg(fare_amount) AS avg_fare
FROM lakehouse.bronze.nyc_taxi_trips GROUP BY trip_date
```

**PySpark (Jupyter):**

```python
q('CREATE TABLE IF NOT EXISTS lakehouse.gold.nyc_taxi_daily_trino AS '
  'SELECT trip_date, count(*) AS trips, avg(fare_amount) AS avg_fare '
  'FROM lakehouse.bronze.nyc_taxi_trips GROUP BY trip_date')
```

### 2.5 Verify

**Scala (Zeppelin):**

```scala
SELECT count(*) FROM lakehouse.gold.nyc_taxi_daily_trino
```

**PySpark (Jupyter):**

```python
q('SELECT count(*) FROM lakehouse.gold.nyc_taxi_daily_trino')
```

## 3. Scala / PySpark parity

Both notebooks share the same numbered sections and produce identical Iceberg tables; only the language and interpreter differ.

## 4. How to run

Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or `jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.
