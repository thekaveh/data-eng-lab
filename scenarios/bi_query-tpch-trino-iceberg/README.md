# bi_query-tpch-trino-iceberg

Query the TPC-H gold-layer marts via Trino SQL — `lakehouse.gold.fct_orders`, `lakehouse.gold.dim_customer` →
`lakehouse.gold.bi_segment_revenue` — from both a Zeppelin `%jdbc(trino)` notebook and a Jupyter notebook
using the `trino` Python client. Both surfaces run identical SQL. Live-gated on Atlas #268.

## 1. Scenario summary
Multi-engine query: Trino reads Iceberg tables written by Spark in the `star_schema` scenario, joins
`fct_orders` and `dim_customer`, and aggregates revenue by market segment — all via standard ANSI SQL.
Demonstrates the lightweight SQL-only query path for complex analytics over the TPC-H lakehouse.

## 2. Why this exists
Shows Trino as a lightweight, multi-engine alternative to Spark for analytical queries on the same Iceberg
lakehouse. The gold marts (`fct_orders`, `dim_customer`) are written by Spark; Trino reads them for BI queries.

## 3. What's in the notebooks
`zeppelin/notebook.zpln` (Trino SQL via `%jdbc(trino)`) and `jupyter/notebook.ipynb` (same SQL via the `trino`
Python client), both with sections Overview→Verify; a `dag.py` EmptyOperator placeholder. Sections walk through
reading the gold tables, aggregating by market segment, writing a summary table, and verifying the result.

## 4. How to run
Run `star_schema-tpch-spark-iceberg` first to create the gold-layer `fct_orders` and `dim_customer` tables.
Then open either notebook on the Atlas stack. The Trino coordinator must be reachable at `trino:8080`.
Trigger the `bi_query_tpch` Airflow DAG once a TrinoOperator integration is added (Atlas #268).

## 5. Data & dependencies
Requires `lakehouse.gold.fct_orders` and `lakehouse.gold.dim_customer` (populated by
`star_schema-tpch-spark-iceberg`); Trino coordinator + Iceberg REST catalog (Atlas A5-A7, issue #268).
The `lakehouse.gold` namespace must be created by running `scripts/register_iceberg.py` before the Write cell executes.

## 6. Known issues & caveats
Live execution is gated on Atlas #268 (Trino coordinator integration). The `%jdbc(trino)` interpreter must
be configured in Zeppelin pointing to the Atlas Trino coordinator. `lakehouse.gold` namespace must exist
in the Iceberg REST catalog before the Write cell runs.
