# data_quality-nyc_taxi-spark-iceberg

Applies data quality checks to NYC taxi trip data using custom Spark SQL functions, counting and flagging records that fail validation rules.

## 1. Purpose

This scenario demonstrates how to implement quality assurance checks as part of a lakehouse ETL pipeline. It defines custom Spark SQL functions for common checks—null detection, range validation, and consistency rules—and applies them to NYC taxi trip data. The output is a summary of quality issues, enabling downstream systems to filter or quarantine low-quality records.

## 2. Data Model

### 2.1 Input Source

Source: `lakehouse.bronze.nyc_taxi_trips` (the bronze table populated by `batch_ingest-nyc_taxi-spark-iceberg`), or local CSV data for the seed.

| Column | Type | Notes |
|---|---|---|
| `VendorID` | double | Vendor identifier |
| `tpep_pickup_datetime` | timestamp | Pickup timestamp |
| `tpep_dropoff_datetime` | timestamp | Dropoff timestamp |
| `passenger_count` | int | Number of passengers |
| `trip_distance` | double | Trip distance in miles |
| `fare_amount` | double | Fare amount |
| `total_amount` | double | Total amount |
| `PULocationID` | int | Pickup location ID |
| `DOLocationID` | int | Dropoff location ID |

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.silver.nyc_taxi_quality` (conceptual) | Silver | All source columns plus quality flag columns and quality_summary |

## 3. Architecture

![Architecture](architectures/data_quality-nyc_taxi-spark-iceberg.html)

NYC taxi trip data (from local CSV seed or the bronze table) flows through Spark batch processing where custom Spark SQL quality-check functions are defined and applied. The output is a quality summary showing counts of records failing various checks (null fields, negative distances, negative fares), with no downstream sink table written.

## 4. Notebooks

- **Zeppelin (Scala):** `zeppelin/notebook.zpln` — Sections: Overview, Read CSV Seed, Define Quality Functions, Apply Quality Checks, Quality Summary, Verify
- **Jupyter (PySpark):** `jupyter/notebook.ipynb` — Same sections; same quality checks expressed using PySpark DataFrame APIs

Both languages define identical quality-check functions (`check_null`, `check_negative_distance`, `check_negative_fare`) and apply them to count and flag problematic records.

## 5. Orchestration

Airflow DAG: `data_quality_nyc_taxi` — a scheduled batch DAG.

## 6. Usage

1. Ensure the `bronze` Iceberg namespace exists: `scripts/register_iceberg.py`
2. Open either notebook on the Atlas stack, or trigger the Airflow DAG:
     ```bash
     airflow dags trigger data_quality_nyc_taxi
     ```
3. Verify:
     ```bash
     spark-sql -e "SELECT * FROM lakehouse.silver.nyc_taxi_quality"
     ```

## 7. Dependencies

- **Dataset:** NYC taxi trips CSV from `s3a://landing/nyc_taxi/taxi_data.csv`
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other:** None

## 8. Known Issues & Caveats

Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4. The 30-minute gap threshold is currently hardcoded as 1800 seconds and not externally configurable. The notebook uses a CSV seed which always re-inserts; drop the target table first for a clean demo.

## See Also

- [Related: batch_ingest-nyc_taxi-spark-iceberg](./batch_ingest-nyc_taxi-spark-iceberg.md) — Produces the source table this scenario consumes
- [Related: medallion-nyc_taxi-spark-iceberg](./medallion-nyc_taxi-spark-iceberg.md) — Transform pipeline that can include quality checks
- [Datasets](../datasets.md)
- [Lakehouse Architecture](../lakehouse.md)
