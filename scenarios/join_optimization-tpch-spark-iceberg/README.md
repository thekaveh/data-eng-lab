# join_optimization-tpch-spark-iceberg

Demonstrates Spark broadcast joins and Adaptive Query Execution by joining TPC-H orders with customer and aggregating revenue by market segment.

## 1. Overview

This scenario explores join optimization strategies in Apache Spark. It joins the TPC-H `orders` and `customer` tables using a broadcast join hint on `o_custkey = c_custkey`, then aggregates total revenue and order count by market segment. The scenario also prints physical plan output via `.explain()` to demonstrate Spark's `BroadcastHashJoin` optimization and the `AQE` scale knob for tuning broadcast thresholds.

## 2. Why This Exists

Understanding join optimization is essential for building performant Spark pipelines on lakehouse data. This scenario shows when to use broadcast joins over sort-merge joins, how AQE automatically selects efficient strategies, and how to inspect physical plans to diagnose and tune join performance at scale.

## 3. Architecture

```
s3a://landing/tpch/orders + s3a://landing/tpch/customer  →  Spark (batch, broadcast join + AQE)  →  lakehouse.gold.tpch_segment_revenue
```

Key components:
- **Source:** TPC-H Parquet (orders, customer) from S3
- **Processing:** Spark (batch)
- **Sink:** `lakehouse.gold.tpch_segment_revenue`
- **Orchestration:** `join_optimization_tpch` Airflow DAG

## 4. Data Schema

### 4.1 Input

Source: TPC-H Parquet datasets in S3

**orders table** (from `s3a://landing/tpch/orders`):
| Column | Type | Notes |
|--------|------|-------|
| `o_orderkey` | long | Order key (PK) |
| `o_custkey` | long | Customer FK |
| `o_totalprice` | double | Order total price |
| `o_orderpriority` | string | Order priority |

**customer table** (from `s3a://landing/tpch/customer`):
| Column | Type | Notes |
|--------|------|-------|
| `c_custkey` | long | Customer key (PK) |
| `c_name` | string | Customer name |
| `c_mktsegment` | string | Market segment |

### 4.2 Output

- **Table:** `lakehouse.gold.tpch_segment_revenue`
- **Layer:** Gold
- **Key columns:** `market_segment`, `total_revenue`, `order_count`

## 5. Notebooks

- **Zeppelin (Scala):** Sections Overview → Verify; reads Parquet from S3, broadcasts customer dimension, joins with orders, aggregates by market segment, prints physical plan, and writes the result.
- **Jupyter (PySpark):** Sections Overview → Verify; same join optimization logic using `broadcast()` hint and `groupBy().agg()` in PySpark, with `.explain()` output.
- Both languages implement identical join optimization logic with broadcast join, AQE demonstration, physical plan inspection, and verification sections.

## 6. How to Run

1. Ensure the `gold` Iceberg namespace exists by running `scripts/register_iceberg.py`.
2. Populate the TPC-H dataset: run `make datasets` to download Parquet files to S3.
3. Open either notebook on the Atlas stack and execute all sections, or trigger the Airflow DAG:
   ```bash
   airflow dags trigger join_optimization_tpch
   ```
4. Verify output:
   ```bash
   spark-sql -e "SELECT * FROM lakehouse.gold.tpch_segment_revenue ORDER BY total_revenue DESC"
   ```

## 7. Dependencies

- **Dataset:** TPC-H Parquet (`orders`, `customer`) from `s3a://landing/tpch/`
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other libraries:** None

## 8. Known Issues & Caveats

- Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4.
- The `gold` namespace must exist in the Iceberg REST catalog; run `scripts/register_iceberg.py` before executing standalone.
- `make datasets` is required to populate the TPC-H landing zone before the notebook can read data.
- This scenario reads directly from S3 landing (no medallion intermediate layers).

## 9. See Also

- [bi_query-tpch-trino-iceberg](../scenarios/bi_query-tpch-trino-iceberg/README.md)
- [star_schema-tpch-spark-iceberg](../scenarios/star_schema-tpch-spark-iceberg/README.md)
- [Datasets](../docs/datasets.md)
- [Lakehouse](../docs/lakehouse.md)
