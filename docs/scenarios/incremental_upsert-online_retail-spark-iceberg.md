# incremental_upsert-online_retail-spark-iceberg

Implements CDC-style incremental upserts using Iceberg's `MERGE INTO` to apply change sets idempotently without rewriting entire tables.

## 1. Purpose

Incremental upserts are essential for building efficient data pipelines that avoid full table rewrites. This scenario teaches how to merge delta batches into an existing Iceberg table while maintaining data consistency and idempotency. The same batch can be merged multiple times without data duplication — a pattern used in daily ETL pipelines and real-time CDC workflows.

## 2. Data Model

### 2.1 Input Source

Source: Online retail batch deltas (inline seed data + change-set batches in the notebooks — no external dataset required).

| Column | Type | Notes |
|---|---|---|
| `InvoiceNo` | string | Invoice number (unique key) |
| `StockCode` | string | Product code |
| `Description` | string | Product description |
| `Quantity` | int | Quantity ordered |
| `InvoiceDate` | timestamp | Invoice date and time |
| `UnitPrice` | double | Price per unit |
| `CustomerID` | double (nullable) | Customer identifier |
| `Country` | string | Customer country |

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.silver.online_retail` | Silver | Same as input; updated and inserted rows reflect latest values |

## 3. Architecture

![Architecture](architectures/incremental_upsert-online_retail-spark-iceberg.html)

Data flows from inline seed data through Spark batch processing with `MERGE INTO`. The notebook seeds initial data, applies two change-set batches (one with an update, one with an insert). The same batch can be merged multiple times without duplication, demonstrating idempotent change-set application.

## 4. Notebooks

- **Zeppelin (Scala):** `zeppelin/notebook.zpln` — Sections: Overview, Seed Data, Merge Batch 1 (update), Merge Batch 2 (insert), Verify Idempotency (merge same batch twice), Verify
- **Jupyter (PySpark):** `jupyter/notebook.ipynb` — Same sections; same CDC logic using PySpark's `df.merge()` and `MERGE INTO` table SQL syntax

Both languages implement identical upsert logic with seeding, merge operations, and verification.

## 5. Orchestration

Airflow DAG: `incremental_upsert_online_retail` — a scheduled batch DAG.

## 6. Usage

1. Ensure the `silver` Iceberg namespace exists: `scripts/register_iceberg.py`
2. Open either notebook on the Atlas stack, or trigger the Airflow DAG:
      ```bash
   airflow dags trigger incremental_upsert_online_retail
      ```
3. Verify:
      ```bash
   spark-sql -e "SELECT InvoiceNo, Description, Quantity FROM lakehouse.silver.online_retail"
      ```

## 7. Dependencies

- **Dataset:** Online retail data (via `make datasets` for registered dataset)
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other:** None

## 8. Known Issues & Caveats

Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4. The `silver` namespace must exist; run `scripts/register_iceberg.py` first. The seed INSERT is not guarded; re-running the full notebook accumulates seed rows. Drop the target table first for a clean demo. At scale, the inline seed can be replaced by the registered `online_retail` dataset.

## See Also

- [Related: scd2-online_retail-spark-iceberg](./scd2-online_retail-spark-iceberg.md) — Another online_retail dimension scenario
- [Related: cdc_streaming-online_retail-spark-iceberg](./cdc_streaming-online_retail-spark-iceberg.md) — Streaming form of CDC upserts
- [Datasets](../datasets.md)
- [Lakehouse Architecture](../lakehouse.md)
