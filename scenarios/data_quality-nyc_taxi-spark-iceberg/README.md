# data_quality-nyc_taxi-spark-iceberg

Validates NYC taxi trip records against business rules, splitting them into clean and quarantine tables based on fare and passenger count constraints.

## 1. Overview

This scenario demonstrates data quality validation on structured data by applying business rules to the NYC taxi trip dataset. Records are split into a clean table (where `fare_amount > 0` AND `passenger_count BETWEEN 1 AND 6`) and a quarantine table (capturing NULL values and rule violations), enabling downstream consumers to use only validated data.

## 2. Why This Exists

Data quality enforcement is a fundamental requirement for reliable data pipelines. This scenario shows how to implement quarantine patterns in a lakehouse, separating valid records from problematic ones so that analytics and ML models consume only clean data while retaining visibility into data quality issues.

## 3. Architecture

```
lakehouse.bronze.nyc_taxi_trips  →  Spark (batch, validation rules)  →  lakehouse.silver.{nyc_taxi_clean, nyc_taxi_quarantine}
```

Key components:
- **Source:** NYC taxi trips from `lakehouse.bronze.nyc_taxi_trips`
- **Processing:** Spark (batch)
- **Sink:** `lakehouse.silver.nyc_taxi_clean`, `lakehouse.silver.nyc_taxi_quarantine`
- **Orchestration:** `data_quality_nyc_taxi` Airflow DAG

## 4. Data Schema

### 4.1 Input

Source: `lakehouse.bronze.nyc_taxi_trips`

| Column | Type | Notes |
|--------|------|-------|
| `fare_amount` | double | Must be > 0 |
| `passenger_count` | int | Must be BETWEEN 1 AND 6 |
| `tpep_pickup_datetime` | timestamp | Pickup time |
| `tpep_dropoff_datetime` | timestamp | Dropoff time |
| Other columns | varied | Passed through to quarantine |

### 4.2 Output

- **Table:** `lakehouse.silver.nyc_taxi_clean`
- **Layer:** Silver
- **Key columns:** All bronze columns for records passing validation rules.

- **Table:** `lakehouse.silver.nyc_taxi_quarantine`
- **Layer:** Silver
- **Key columns:** All bronze columns plus a `reason` column indicating which rule was violated (NULL fare, invalid passenger count).

## 5. Notebooks

- **Zeppelin (Scala):** Sections Overview → Verify; reads bronze table, applies business rules via `filter`/`when`, splits into clean and quarantine DataFrames, writes both to Silver, and verifies row counts.
- **Jupyter (PySpark):** Sections Overview → Verify; same data quality logic using PySpark DataFrame filtering, conditional logic with `when()`, and dual sink writes.
- Both languages implement identical quality validation logic with rule definition, conditional splitting, dual sink writes, and verification sections.

## 6. How to Run

1. Ensure the `silver` and `bronze` Iceberg namespaces exist by running `scripts/register_iceberg.py`.
2. Populate the NYC taxi bronze table by running the prerequisite `batch_ingest-nyc_taxi-spark-iceberg` scenario.
3. Open either notebook on the Atlas stack and execute all sections, or trigger the Airflow DAG:
   ```bash
   airflow dags trigger data_quality_nyc_taxi
   ```
4. Verify output:
   ```bash
   spark-sql -e "SELECT COUNT(*) AS clean_count FROM lakehouse.silver.nyc_taxi_clean"
   spark-sql -e "SELECT COUNT(*) AS quarantine_count FROM lakehouse.silver.nyc_taxi_quarantine"
   ```

## 7. Dependencies

- **Dataset:** NYC taxi trips (via `lakehouse.bronze.nyc_taxi_trips`)
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other libraries:** None

## 8. Known Issues & Caveats

- Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4.
- The `silver` namespace must exist in the Iceberg REST catalog; run `scripts/register_iceberg.py` before executing standalone.
- Requires prerequisite: `batch_ingest-nyc_taxi-spark-iceberg` must run first to populate `lakehouse.bronze.nyc_taxi_trips`.

## 9. See Also

- upstream: [batch_ingest-nyc_taxi-spark-iceberg](../scenarios/batch_ingest-nyc_taxi-spark-iceberg/README.md)
- [Datasets](../docs/datasets.md)
- [Lakehouse](../docs/lakehouse.md)
