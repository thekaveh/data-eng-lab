<!-- AUTO-GENERATED — do not edit; run scripts/build_docs.py -->
# Notebooks — bi_query-tpch-trino-iceberg
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
from trino.dbapi import connect

cur = connect(host='trino', port=8080, user='atlas', catalog='lakehouse').cursor()
def q(sql):
    cur.execute(sql)
    return cur.fetchall()
```

## 2. Setup

### 3. Read

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
q('SELECT * FROM lakehouse.gold.fct_orders LIMIT 10')
```

## 3. Read

### 4. Transform

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
q('SELECT c.c_mktsegment, sum(f.revenue) AS revenue, sum(f.line_count) AS lines '
  'FROM lakehouse.gold.fct_orders f '
  'JOIN lakehouse.gold.dim_customer c ON f.o_custkey = c.c_custkey '
  'GROUP BY c.c_mktsegment ORDER BY revenue DESC')
```

## 4. Transform

### 5. Write

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
q('CREATE TABLE IF NOT EXISTS lakehouse.gold.bi_segment_revenue AS '
  'SELECT c.c_mktsegment, sum(f.revenue) AS revenue '
  'FROM lakehouse.gold.fct_orders f '
  'JOIN lakehouse.gold.dim_customer c ON f.o_custkey = c.c_custkey '
  'GROUP BY c.c_mktsegment')
```

## 5. Write

### 6. Verify

**Scala (Zeppelin):**

```scala

```

**PySpark (Jupyter):**

```python
q('SELECT count(*) FROM lakehouse.gold.bi_segment_revenue')
```

## 6. Verify

## 4. Scala / PySpark parity

Both notebooks share the same numbered sections and produce identical Iceberg tables; only the language and interpreter differ.

## 5. How to run

Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or `jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.
