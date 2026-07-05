# scd2-online_retail-spark-iceberg

Implements Slowly Changing Dimension Type 2 (SCD2) to track historical changes on a customer dimension, preserving full history with effective timestamps and current flags.

## 1. Overview

This scenario demonstrates SCD2 dimensional modeling using Iceberg's row-level UPDATE and INSERT capabilities. It seeds a customer dimension, then simulates a segment change by closing the old row (setting `effective_to` and `is_current=false`) and opening a new row with the updated segment (setting a new `effective_from` and `is_current=true`).

## 2. Why This Exists

SCD2 is a cornerstone of dimensional data warehousing, enabling time-travel queries on dimension changes. This scenario shows how Iceberg's native row-level UPDATE support makes SCD2 practical, allowing efficient history tracking without full table scans or partition rewrites.

## 3. Architecture

```
Online retail dimension  →  Spark (batch, SCD2 logic)  →  lakehouse.gold.dim_customer_scd2 (in-place)
```

Key components:
- **Source:** Online retail dimension data (inline seed)
- **Processing:** Spark (batch)
- **Sink:** `lakehouse.gold.dim_customer_scd2` (in-place)
- **Orchestration:** `scd2_online_retail` Airflow DAG

## 4. Data Schema

### 4.1 Input

Source: Online retail dimension data (inline seed)

| Column | Type | Notes |
|--------|------|-------|
| `CustomerID` | double | Customer identifier |
| `Name` | string | Customer name |
| `Segment` | string | Customer segment |

### 4.2 Output

- **Table:** `lakehouse.gold.dim_customer_scd2`
- **Layer:** Gold
- **Key columns:** `CustomerID`, `Name`, `Segment`, `effective_from`, `effective_to`, `is_current`

## 5. Notebooks

- **Zeppelin (Scala):** Sections Overview → Verify; seeds initial customer data, applies SCD2 logic for a segment change (close old row, open new row), and queries historical and current records.
- **Jupyter (PySpark):** Sections Overview → Verify; same SCD2 logic using PySpark `when()` and DataFrame updates with `effective_from`/`effective_to`/`is_current` tracking.
- Both languages implement identical SCD2 logic with seeding, history tracking via effective timestamps and current flags, and verification sections.

## 6. How to Run

1. Ensure the `gold` Iceberg namespace exists by running `scripts/register_iceberg.py`.
2. Open either notebook on the Atlas stack and execute all sections, or trigger the Airflow DAG:
   ```bash
   airflow dags trigger scd2_online_retail
   ```
3. Verify output:
   ```bash
   spark-sql -e "SELECT * FROM lakehouse.gold.dim_customer_scd2"
   ```

## 7. Dependencies

- **Dataset:** Online retail dimension data (inline seed)
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other libraries:** None

## 8. Known Issues & Caveats

- Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4.
- The `gold` namespace must exist in the Iceberg REST catalog; run `scripts/register_iceberg.py` before executing standalone.
- Iceberg row-level UPDATE is an SQL extension; ensure `iceberg.sql.extensions` is enabled in Spark configuration.
- The notebook's seed INSERT is not guarded; re-running the full notebook accumulates seed rows. Drop the target table first for a clean demo.

## 9. See Also

- [incremental_upsert-online_retail-spark-iceberg](../scenarios/incremental_upsert-online_retail-spark-iceberg/README.md)
- [cdc_streaming-online_retail-spark-iceberg](../scenarios/cdc_streaming-online_retail-spark-iceberg/README.md)
- [Datasets](../docs/datasets.md)
- [Lakehouse](../docs/lakehouse.md)
