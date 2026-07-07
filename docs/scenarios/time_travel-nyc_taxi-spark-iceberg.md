# time_travel-nyc_taxi-spark-iceberg

Demonstrates Iceberg time travel capabilities — querying historical table versions using `VERSION AS OF` and `TIMESTAMP AS OF` syntax — on NYC taxi trip data.

## 1. Purpose

Iceberg's time travel feature is a differentiator from traditional data warehouse tables. It allows querying the table as it existed at a previous point in time, either by version number or by timestamp. This scenario demonstrates time travel on NYC taxi trip data, showing how operations like inserts, overwrites, and appends create a version history that can be queried retrospectively.

## 2. Data Model

### 2.1 Input Source

Source: `s3a://landing/nyc_taxi/taxi_data.csv` (local CSV seed) plus NYC Taxi Trips Parquet data.

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
| `lakehouse.silver.time_travel_demo` | Silver | Demonstrates time travel via version and timestamp queries |

## 3. Architecture

![Architecture](architectures/time_travel-nyc_taxi-spark-iceberg.html)

NYC taxi trip data flows through Spark batch processing where the table undergoes multiple write operations (inserts and overwrites). After each operation, the table acquires a new snapshot. Time travel queries then read specific historical snapshots using either `VERSION AS OF <version>` or `TIMESTAMP AS OF <timestamp>`, demonstrating point-in-time accuracy. The scenario also explores Write-Audit-Publish (WAP) branching: create a WAP branch for safe mutation, validate reads against it, then fast-forward the branch to publish — all without affecting the main branch until the changes are ready.

## 4. Notebooks

- **Zeppelin (Scala):** `zeppelin/notebook.zpln` — Sections: Overview, Seed Table, Apply Multiple Changes, Time Travel by Version, Time Travel by Timestamp, Verify
- **Jupyter (PySpark):** `jupyter/notebook.ipynb` — Same sections; same time travel logic using PySpark with `VERSION AS OF` and `TIMESTAMP AS OF` syntax

Both languages implement identical time travel logic with multiple write operations and version/timestamp-based historical queries.

## 5. Orchestration

Airflow DAG: `time_travel_nyc_taxi` — a scheduled batch DAG.

## 6. Usage

1. Ensure the `silver` Iceberg namespace exists: `scripts/register_iceberg.py`
2. Open either notebook on the Atlas stack, or trigger the Airflow DAG:
     ```bash
     airflow dags trigger time_travel_nyc_taxi
     ```
3. Verify:
     ```bash
     spark-sql -e "SELECT * FROM lakehouse.silver.time_travel_demo LIMIT 10"
     ```

## 7. Dependencies

- **Dataset:** NYC Taxi Trips CSV Parquet from `s3a://landing/nyc_taxi/`
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other:** Iceberg time travel must be enabled (default configuration)

## 8. Known Issues & Caveats

Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4. The `silver` namespace must exist; run `scripts/register_iceberg.py` first. Time travel snapshots are retained based on Iceberg retention settings — old snapshots may be expired by `VACUUM`. The notebook performs multiple operations on the same table, creating multiple snapshots for time travel demonstration.

## See Also

- [Related: batch_ingest-nyc_taxi-spark-iceberg](./batch_ingest-nyc_taxi-spark-iceberg.md) — Produces the bronze source data
- [Related: table_maintenance-nyc_taxi-spark-iceberg](./table_maintenance-nyc_taxi-spark-iceberg.md) — Also demonstrates time travel
- [Related: medallion-nyc_taxi-spark-iceberg](./medallion-nyc_taxi-spark-iceberg.md) — Full medallion pipeline
- [Production Spark app: nyc-taxi-medallion](../spark-apps/nyc-taxi-medallion.md) — Phase-3a JAR productionizes this scenario for Airflow
- [Datasets](../datasets.md)
- [Lakehouse Architecture](../lakehouse.md)
