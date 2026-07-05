# incremental_upsert-online_retail-spark-iceberg

Implements CDC-style incremental upserts using Iceberg's `MERGE INTO` to apply change sets idempotently without rewriting entire tables.

## 1. Overview

This scenario demonstrates change data capture (CDC) upserts with Apache Iceberg's `MERGE INTO` syntax. It seeds the table with initial data, then applies two batches: one containing an update and an insert. The same batch can be merged multiple times without data duplication, demonstrating idempotent change-set application.

## 2. Why This Exists

Incremental upserts are essential for building efficient data pipelines that avoid full table rewrites. This scenario teaches how to merge delta batches into an existing Iceberg table while maintaining data consistency and idempotency, a pattern used in daily ETL pipelines and real-time CDC workflows.

## 3. Architecture

```
Online retail deltas  →  Spark (batch, MERGE INTO)  →  lakehouse.silver.online_retail (in-place)
```

Key components:
- **Source:** Online retail batch deltas (inline seed + change set)
- **Processing:** Spark (batch)
- **Sink:** `lakehouse.silver.online_retail` (in-place merge)
- **Orchestration:** `incremental_upsert_online_retail` Airflow DAG

## 4. Data Schema

### 4.1 Input

Source: Online retail batch deltas (inline seed data and change-set batches)

| Column | Type | Notes |
|--------|------|-------|
| `InvoiceNo` | string | Invoice number (unique key) |
| `StockCode` | string | Product code |
| `Description` | string | Product description |
| `Quantity` | int | Quantity ordered |
| `InvoiceDate` | timestamp | Invoice date and time |
| `UnitPrice` | double | Price per unit |
| `CustomerID` | double (nullable) | Customer identifier |
| `Country` | string | Customer country |

### 4.2 Output

- **Table:** `lakehouse.silver.online_retail`
- **Layer:** Silver
- **Key columns:** Same as input; updated and inserted rows reflect latest values.

## 5. Notebooks

- **Zeppelin (Scala):** Sections Overview → Verify; seeds initial data, demonstrates MERGE INTO for update and insert, verifies idempotency by merging the same batch twice.
- **Jupyter (PySpark):** Sections Overview → Verify; same CDC logic using PySpark's `df.merge()` and `MERGE INTO` table SQL syntax.
- Both languages implement identical upsert logic with seeding, merge operations, and verification sections.

## 6. How to Run

1. Ensure the `silver` Iceberg namespace exists by running `scripts/register_iceberg.py`.
2. Open either notebook on the Atlas stack and execute all sections, or trigger the Airflow DAG:
   ```bash
   airflow dags trigger incremental_upsert_online_retail
   ```
3. Verify output:
   ```bash
   spark-sql -e "SELECT InvoiceNo, Description, Quantity FROM lakehouse.silver.online_retail"
   ```

## 7. Dependencies

- **Dataset:** Online retail data (via `make datasets` for the registered dataset)
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other libraries:** None

## 8. Known Issues & Caveats

- Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4.
- The `silver` namespace must exist in the Iceberg REST catalog; run `scripts/register_iceberg.py` before executing standalone.
- The notebook's seed INSERT is not guarded; re-running the full notebook accumulates seed rows. Drop the target table first for a clean demo.
- At scale, the inline seed can be replaced by the registered `online_retail` dataset.

## 9. See Also

- [scd2-online_retail-spark-iceberg](../scenarios/scd2-online_retail-spark-iceberg/README.md)
- [cdc_streaming-online_retail-spark-iceberg](../scenarios/cdc_streaming-online_retail-spark-iceberg/README.md)
- [Datasets](../docs/datasets.md)
- [Lakehouse](../docs/lakehouse.md)
