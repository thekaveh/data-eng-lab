# table_maintenance-nyc_taxi-spark-iceberg

Demonstrates Iceberg table maintenance operations: overwrite, VACUUM, and time travel on NYC taxi trip data.

## 1. Purpose

Iceberg provides powerful table maintenance operations that are essential for production lakehouse management: time travel (querying historical table versions), overwriting data by partition (efficiently replacing bad or outdated partitions), and VACUUM (removing orphan files and cleaning up old snapshots). This scenario shows all three operations in action on real taxi trip data.

## 2. Data Model

### 2.1 Input Source

Source: `lakehouse.bronze.nyc_taxi_trips` (populated by `batch_ingest-nyc_taxi-spark-iceberg`).

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
| `lakehouse.silver.maintenance_demo` | Silver | Demonstrates overwrite, VACUUM, and time travel |

## 3. Architecture

![Architecture](../../docs/scenarios/architectures/table_maintenance-nyc_taxi-spark-iceberg.html)

NYC taxi trip data from the bronze table flows through Spark batch processing demonstrating three Iceberg maintenance operations: (1) partition overwrite — replacing a specific date partition with new data, (2) VACUUM — cleaning up orphan metadata and data files, and (3) time travel — querying historical versions of the table using version or timestamp.

## 4. Notebooks

- **Zeppelin (Scala):** `zeppelin/notebook.zpln` — Sections: Overview, Create Table with Seed Data, Apply Changes (overwrite a partition), Time Travel (query previous version), VACUUM (clean up orphan files), Verify
- **Jupyter (PySpark):** `jupyter/notebook.ipynb` — Same sections; same maintenance operations using PySpark with `OPTION (overwritePartitions = true)`, `VACUUM`, and time travel syntax `VERSION AS OF` / `TIMESTAMP AS OF`

Both languages implement identical maintenance operations: seed data insertion, partition overwrite, time travel, and VACUUM.

## 5. Orchestration

Airflow DAG: `table_maintenance_nyc_taxi` — a scheduled batch DAG.

## 6. Usage

1. Ensure the `silver` and `gold` Iceberg namespaces exist: `scripts/register_iceberg.py`
2. Open either notebook on the Atlas stack, or trigger the Airflow DAG:
     ```bash
     airflow dags trigger table_maintenance_nyc_taxi
     ```
3. Verify:
     ```bash
     spark-sql -e "SELECT COUNT(*) FROM lakehouse.silver.maintenance_demo"
     ```

## 7. Dependencies

- **Dataset:** NYC taxi trip data (via `lakehouse.bronze.nyc_taxi_trips` populated by batch_ingest)
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other:** Iceberg table maintenance must be enabled in configuration

## 8. Known Issues & Caveats

Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4. Both `silver` and `gold` namespaces must exist; run `scripts/register_iceberg.py` first. VACUUM retention is set to safety minimums — do not set retention below 1008 minutes (1 day) in production. The `retainLast(1)` ensures at least one history version is always kept.

## See Also

- [Related: batch_ingest-nyc_taxi-spark-iceberg](../batch_ingest-nyc_taxi-spark-iceberg/README.md) — Produces the bronze source data
- [Related: medallion-nyc_taxi-spark-iceberg](../medallion-nyc_taxi-spark-iceberg/README.md) — Full medallion pipeline
- [Datasets](../../docs/datasets.md)
- [Lakehouse Architecture](../../docs/lakehouse.md)
