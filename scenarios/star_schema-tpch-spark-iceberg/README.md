# star_schema-tpch-spark-iceberg

Builds fact and dimension tables from the TPC-H dataset using star schema dimensional modeling, creating `dim_customer` and `fct_orders` in the gold layer.

## 1. Overview

This scenario implements dimensional modeling by reading the TPC-H `orders`, `customer`, and `lineitem` tables from S3, joining orders with lineitems on order key, and aggregating by order, customer, and date dimensions. The result produces a star schema with a `dim_customer` dimension table and a `fct_orders` fact table, both stored as Iceberg tables.

## 2. Why This Exists

Star schema design is the foundation of dimensional data warehousing. This scenario shows how to implement it in Spark over a lakehouse, connecting source tables into a structured model optimized for analytical queries and BI tool consumption.

## 3. Architecture

```
s3a://landing/tpch/{orders, customer, lineitem}  →  Spark (batch, star schema model)  →  lakehouse.gold.{dim_customer, fct_orders}
```

Key components:
- **Source:** TPC-H Parquet (orders, customer, lineitem) from S3
- **Processing:** Spark (batch)
- **Sink:** `lakehouse.gold.dim_customer`, `lakehouse.gold.fct_orders`
- **Orchestration:** `star_schema_tpch` Airflow DAG

## 4. Data Schema

### 4.1 Input

Source: TPC-H Parquet datasets in S3

**orders table** (from `s3a://landing/tpch/orders`):
| Column | Type | Notes |
|--------|------|-------|
| `o_orderkey` | long | Order key (FK in fact) |
| `o_custkey` | long | Customer key (FK to dim) |
| `o_totalprice` | double | Order total |
| `o_orderstatus` | string | Order status |

**customer table** (from `s3a://landing/tpch/customer`):
| Column | Type | Notes |
|--------|------|-------|
| `c_custkey` | long | Customer key (PK) |
| `c_name` | string | Customer name |
| `c_mktsegment` | string | Market segment |

**lineitem table** (from `s3a://landing/tpch/lineitem`):
| Column | Type | Notes |
|--------|------|-------|
| `l_orderkey` | long | Order key (FK) |
| `l_quantity` | double | Line item quantity |
| `l_extendedprice` | double | Line item extended price |

### 4.2 Output

- **Table:** `lakehouse.gold.dim_customer`
- **Layer:** Gold (dimension)
- **Key columns:** `c_custkey`, `c_name`, `c_mktsegment`

- **Table:** `lakehouse.gold.fct_orders`
- **Layer:** Gold (fact)
- **Key columns:** `o_orderkey`, `o_custkey`, `o_orderstatus`, `o_totalprice`, `l_quantity`, `l_extendedprice`

## 5. Notebooks

- **Zeppelin (Scala):** Sections Overview → Verify; reads Parquet from S3, joins orders with lineitems and customer, creates dimension and fact tables, writes to gold layer, and verifies schema and row counts.
- **Jupyter (PySpark):** Sections Overview → Verify; same star schema logic using PySpark DataFrame joins with dimension construction and fact table aggregation.
- Both languages implement identical dimensional modeling logic with source ingestion, multi-table joins, dimension/fact table creation, and verification sections.

## 6. How to Run

1. Ensure the `gold` Iceberg namespace exists by running `scripts/register_iceberg.py`.
2. Populate the TPC-H dataset: run `make datasets` to download Parquet files to S3.
3. Open either notebook on the Atlas stack and execute all sections, or trigger the Airflow DAG:
   ```bash
   airflow dags trigger star_schema_tpch
   ```
4. Verify output:
   ```bash
   spark-sql -e "SELECT COUNT(*) FROM lakehouse.gold.dim_customer"
   spark-sql -e "SELECT COUNT(*) FROM lakehouse.gold.fct_orders"
   ```

## 7. Dependencies

- **Dataset:** TPC-H Parquet (`orders`, `customer`, `lineitem`) from `s3a://landing/tpch/`
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other libraries:** None

## 8. Known Issues & Caveats

- Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4.
- The `gold` namespace must exist in the Iceberg REST catalog; run `scripts/register_iceberg.py` before executing standalone.
- `make datasets` is required to populate the TPC-H landing zone before the notebook can read data.
- This scenario reads directly from S3 landing (no medallion intermediate layers).

## 9. See Also

- [bi_query-tpch-trino-iceberg](../scenarios/bi_query-tpch-trino-iceberg/README.md) — reads gold marts produced by this scenario
- [join_optimization-tpch-spark-iceberg](../scenarios/join_optimization-tpch-spark-iceberg/README.md)
- [Datasets](../docs/datasets.md)
- [Lakehouse](../docs/lakehouse.md)
