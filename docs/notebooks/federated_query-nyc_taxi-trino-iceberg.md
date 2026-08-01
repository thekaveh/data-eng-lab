# 6.17. federated_query-nyc_taxi-trino-iceberg
Documents the scenario's paired Jupyter (`notebook.ipynb`) and Zeppelin (`notebook.zpln`) implementations.
Both notebooks implement the same Trino queries through different clients.

## 1. Section map

| Subsection | Trino SQL (Zeppelin) | Python client (Jupyter) |
|---|---|---|
| 2.1 Setup | ✓ | ✓ |
| 2.2 Read | ✓ | ✓ |
| 2.3 Transform | ✓ | ✓ |
| 2.4 Write | ✓ | ✓ |
| 2.5 Verify | ✓ | ✓ |

## 2. Walkthrough

### 2.1 Setup

**Trino SQL (Zeppelin):**

```sql
-- %trino is pre-bound to the Atlas Trino coordinator (catalog: lakehouse)
```

**Python client (Jupyter):**

```python
from trino.dbapi import connect

cur = connect(host='trino', port=8080, user='atlas', catalog='lakehouse').cursor()
def q(sql):
    cur.execute(sql)
    return cur.fetchall()
```

### 2.2 Read

**Trino SQL (Zeppelin):**

```sql
SELECT * FROM lakehouse.bronze.nyc_taxi_trips LIMIT 10
```

**Python client (Jupyter):**

```python
q('SELECT * FROM lakehouse.bronze.nyc_taxi_trips LIMIT 10')
```

### 2.3 Transform

**Trino SQL (Zeppelin):**

```sql
SELECT trip_date, count(*) AS trips, avg(fare_amount) AS avg_fare
FROM lakehouse.bronze.nyc_taxi_trips
GROUP BY trip_date ORDER BY trip_date
```

**Python client (Jupyter):**

```python
q('SELECT trip_date, count(*) AS trips, avg(fare_amount) AS avg_fare '
  'FROM lakehouse.bronze.nyc_taxi_trips GROUP BY trip_date ORDER BY trip_date')
```

### 2.4 Write

**Trino SQL (Zeppelin):**

```sql
CREATE TABLE IF NOT EXISTS lakehouse.gold.nyc_taxi_daily_trino AS
SELECT trip_date, count(*) AS trips, avg(fare_amount) AS avg_fare
FROM lakehouse.bronze.nyc_taxi_trips GROUP BY trip_date
```

**Python client (Jupyter):**

```python
q('CREATE TABLE IF NOT EXISTS lakehouse.gold.nyc_taxi_daily_trino AS '
  'SELECT trip_date, count(*) AS trips, avg(fare_amount) AS avg_fare '
  'FROM lakehouse.bronze.nyc_taxi_trips GROUP BY trip_date')
```

### 2.5 Verify

**Trino SQL (Zeppelin):**

```sql
SELECT count(*) FROM lakehouse.gold.nyc_taxi_daily_trino
```

**Python client (Jupyter):**

```python
q('SELECT count(*) FROM lakehouse.gold.nyc_taxi_daily_trino')
```

## 3. Trino query equivalence

Both notebooks share the same numbered sections and issue equivalent Trino SQL. The Zeppelin notebook uses `%trino`; Jupyter uses the Python DB-API client. These two scenarios are execution-gated but are not included in the 17 Scala/PySpark parity pairs.

## 4. How to run

Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or `jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.
