# bi_query-tpch-trino-iceberg

Queries gold-layer marts via Trino SQL, demonstrating Trino as a lightweight SQL-only analytics engine over Iceberg tables produced by Spark.

## 1. Overview

This scenario demonstrates multi-engine querying of the lakehouse by using Trino to query gold-layer tables written by Spark's `star_schema` scenario. It reads `fct_orders` and `dim_customer`, joins them to aggregate revenue by market segment, and writes a summary table. Both a Zeppelin `%trino` notebook and a Jupyter notebook using the `trino` Python client run identical SQL queries.

## 2. Why This Exists

Trino provides a lightweight, SQL-only query path over lakehouse data that complements Spark's programmatic ETL. This scenario shows how different engines can share the same underlying Iceberg tables, enabling analysts to query data without writing Spark code and demonstrating true multi-engine lakehouse architecture.

## 3. Architecture

```
lakehouse.gold.{fct_orders, dim_customer}  →  Trino (SQL)  →  lakehouse.gold.bi_segment_revenue
```

Key components:
- **Source:** TPC-H gold tables (`fct_orders`, `dim_customer`) from `lakehouse.gold`
- **Processing:** Trino (SQL, batch)
- **Sink:** `lakehouse.gold.bi_segment_revenue`
- **Orchestration:** Placeholder (EmptyOperator)

## 4. Data Schema

### 4.1 Input

Source: `lakehouse.gold` tables written by `star_schema-tpch-spark-iceberg`

From `lakehouse.gold.fct_orders`:
| Column | Type | Notes |
|--------|------|-------|
| `o_orderkey` | long | Order key |
| `o_custkey` | long | Customer FK |
| `o_totalprice` | double | Order total |

From `lakehouse.gold.dim_customer`:
| Column | Type | Notes |
|--------|------|-------|
| `c_custkey` | long | Customer PK |
| `c_name` | string | Customer name |
| `c_mktsegment` | string | Market segment |

### 4.2 Output

- **Table:** `lakehouse.gold.bi_segment_revenue`
- **Layer:** Gold
- **Key columns:** `market_segment`, `total_revenue`, `order_count`

## 5. Notebooks

- **Zeppelin (Scala, `%trino`):** Sections Overview → Verify; runs SQL queries to read gold tables, joins `fct_orders` with `dim_customer`, aggregates by market segment, writes summary, and verifies results.
- **Jupyter (Py, `trino` client):** Sections Overview → Verify; identical SQL queries executed via the Trino Python client connecting to `trino:8080`.
- Both notebooks run the same SQL to demonstrate cross-engine parity for analytical queries.

## 6. How to Run

1. Run the prerequisite scenario `star_schema-tpch-spark-iceberg` to create `fct_orders` and `dim_customer`.
2. Ensure the `gold` Iceberg namespace exists by running `scripts/register_iceberg.py`.
3. Open either notebook on the Atlas stack (Trino coordinator must be reachable at `trino:8080`) and execute all sections.
4. The Airflow DAG is a placeholder (`EmptyOperator`); trigger via notebooks only until TrinoOperator integration is added (Atlas #268).

## 7. Dependencies

- **Dataset:** TPC-H gold tables (`fct_orders`, `dim_customer`) from `lakehouse.gold`
- **Atlas services:** A5-A7 (Trino, Trino coordinator, Iceberg REST catalog)
- **Other libraries:** `trino` Python client (Jupyter)

## 8. Known Issues & Caveats

- Live execution is gated on Atlas #268 (Trino coordinator integration).
- The `%trino` Zeppelin interpreter is seeded by Atlas pointing to the Trino coordinator.
- `lakehouse.gold` namespace must exist in the Iceberg REST catalog before the Write query runs.
- Requires prerequisite: `star_schema-tpch-spark-iceberg` must run first to populate gold-layer source tables.
- Airflow DAG uses a placeholder `EmptyOperator`; actual scheduling requires TrinoOperator integration.

## 9. See Also

- upstream: [star_schema-tpch-spark-iceberg](../scenarios/star_schema-tpch-spark-iceberg/README.md)
- related: [join_optimization-tpch-spark-iceberg](../scenarios/join_optimization-tpch-spark-iceberg/README.md)
- [Datasets](../docs/datasets.md)
- [Lakehouse](../docs/lakehouse.md)
