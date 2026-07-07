<!-- AUTO-GENERATED — do not edit; run scripts/build_docs.py -->
# Notebooks — bi_query-tpch-trino-iceberg
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
SELECT * FROM lakehouse.gold.fct_orders LIMIT 10
```

**PySpark (Jupyter):**

```python
q('SELECT * FROM lakehouse.gold.fct_orders LIMIT 10')
```

### 2.3 Transform

**Scala (Zeppelin):**

```scala
SELECT c.c_mktsegment, sum(f.revenue) AS revenue, sum(f.line_count) AS lines
FROM lakehouse.gold.fct_orders f
JOIN lakehouse.gold.dim_customer c ON f.o_custkey = c.c_custkey
GROUP BY c.c_mktsegment ORDER BY revenue DESC
```

**PySpark (Jupyter):**

```python
q('SELECT c.c_mktsegment, sum(f.revenue) AS revenue, sum(f.line_count) AS lines '
  'FROM lakehouse.gold.fct_orders f '
  'JOIN lakehouse.gold.dim_customer c ON f.o_custkey = c.c_custkey '
  'GROUP BY c.c_mktsegment ORDER BY revenue DESC')
```

### 2.4 Write

**Scala (Zeppelin):**

```scala
CREATE TABLE IF NOT EXISTS lakehouse.gold.bi_segment_revenue AS
SELECT c.c_mktsegment, sum(f.revenue) AS revenue
FROM lakehouse.gold.fct_orders f
JOIN lakehouse.gold.dim_customer c ON f.o_custkey = c.c_custkey
GROUP BY c.c_mktsegment
```

**PySpark (Jupyter):**

```python
q('CREATE TABLE IF NOT EXISTS lakehouse.gold.bi_segment_revenue AS '
  'SELECT c.c_mktsegment, sum(f.revenue) AS revenue '
  'FROM lakehouse.gold.fct_orders f '
  'JOIN lakehouse.gold.dim_customer c ON f.o_custkey = c.c_custkey '
  'GROUP BY c.c_mktsegment')
```

### 2.5 Verify

**Scala (Zeppelin):**

```scala
SELECT count(*) FROM lakehouse.gold.bi_segment_revenue
```

**PySpark (Jupyter):**

```python
q('SELECT count(*) FROM lakehouse.gold.bi_segment_revenue')
```

## 3. Scala / PySpark parity

Both notebooks share the same numbered sections and produce identical Iceberg tables; only the language and interpreter differ.

## 4. How to run

Open the scenario's `zeppelin/notebook.zpln` on the Atlas Zeppelin UI or `jupyter/notebook.ipynb` on JupyterHub, then run all paragraphs/cells top to bottom.
