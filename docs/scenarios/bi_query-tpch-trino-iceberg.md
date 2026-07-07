# bi_query-tpch-trino-iceberg

Queries gold-layer marts via Trino SQL, demonstrating Trino as a lightweight SQL-only analytics engine over Iceberg tables produced by Spark.

## 1. Purpose

Trino provides a lightweight, SQL-only query path over lakehouse data that complements Spark's programmatic ETL. This scenario shows how different engines can share the same underlying Iceberg tables: analysts can query data without writing Spark code. It reads `fct_orders` and `dim_customer` from the `star_schema` scenario, joins them, aggregates revenue by market segment, and writes a summary table — demonstrating true multi-engine lakehouse architecture.

## 2. Data Model

### 2.1 Input Source

Source: `lakehouse.gold` tables written by the upstream `star_schema-tpch-spark-iceberg` scenario.

From `lakehouse.gold.fct_orders`:

| Column | Type | Notes |
|---|---|---|
| `o_orderkey` | long | Order key |
| `o_custkey` | long | Customer FK |
| `o_totalprice` | double | Order total |

From `lakehouse.gold.dim_customer`:

| Column | Type | Notes |
|---|---|---|
| `c_custkey` | long | Customer PK |
| `c_name` | string | Customer name |
| `c_mktsegment` | string | Market segment |

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.gold.bi_segment_revenue` | Gold | `market_segment`, `total_revenue`, `order_count` |

## 3. Architecture

![Architecture](architectures/bi_query-tpch-trino-iceberg.html)

Data flows from gold-layer Iceberg tables (`fct_orders`, `dim_customer`) through Trino SQL queries. The Trino coordinator connects to the Iceberg catalog, reads the gold tables, joins them, aggregates revenue by market segment, and writes the summary back to the gold layer — all via standard ANSI SQL with no Spark involvement.

## 4. Notebooks

- **Zeppelin (Scala, `%trino`):** Sections: Overview, Read Gold Tables, Join + Aggregate, Write Summary, Verify; identical SQL to PySpark
- **Jupyter (Py, `trino` client):** Sections: Overview, Read Gold Tables, Join + Aggregate, Write Summary, Verify; identical SQL executed via the Trino Python client connecting to `trino:8080`

Both notebooks run the same SQL queries to demonstrate cross-engine parity for analytical queries.

## 5. Orchestration

Airflow DAG: EmptyOperator placeholder (trigger via notebooks only until TrinoOperator integration is added, Atlas #268).

## 6. Usage

1. Run the prerequisite scenario: `star_schema-tpch-spark-iceberg` (creates `fct_orders` and `dim_customer`)
2. Ensure the `gold` Iceberg namespace exists: `scripts/register_iceberg.py`
3. Open either notebook on the Atlas stack (Trino coordinator must be reachable at `trino:8080`) and run all sections
4. Verify:
     ```bash
   spark-sql -e "SELECT * FROM lakehouse.gold.bi_segment_revenue ORDER BY total_revenue DESC"
     ```

## 7. Dependencies

- **Dataset:** TPC-H gold tables (`fct_orders`, `dim_customer`) from `lakehouse.gold`
- **Atlas services:** A5-A7 (Trino, Trino coordinator, Iceberg REST catalog)
- **Other:** `trino` Python client (Jupyter notebook)

## 8. Known Issues & Caveats

Live execution is gated on Atlas #268 (Trino coordinator integration). The `%trino` Zeppelin interpreter is seeded by Atlas pointing to the Trino coordinator. The `lakehouse.gold` namespace must exist before the Write query runs. Requires the upstream `star_schema-tpch-spark-iceberg` to run first.

## See Also

- [Upstream: star_schema-tpch-spark-iceberg](./star_schema-tpch-spark-iceberg.md) — Populates the gold tables this scenario queries
- [Related: join_optimization-tpch-spark-iceberg](./join_optimization-tpch-spark-iceberg.md) — Another TPC-H query optimization scenario
- [Datasets](../datasets.md)
- [Lakehouse Architecture](../lakehouse.md)
