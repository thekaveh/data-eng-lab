# medallion-nyc_taxi-spark-iceberg

Transforms raw NYC taxi trip data from the bronze layer through silver (cleaned) to gold (aggregated), implementing the medallion architecture pattern.

## 1. Purpose

This scenario demonstrates the full medallion pattern: raw data is ingested into bronze, cleaned and typed in silver, and aggregated in gold for BI consumption. It models the real-world pattern seen in production lakehouses — progressive refinement of data quality and granularity through each layer. The bronze layer preserves the raw data, silver cleans and adds computed columns (duration, direction), and gold produces aggregated metrics by location and time.

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
| `lakehouse.silver.nyc_taxi_trips` | Silver | Same as bronze + `trip_duration`, `start_hour`, `trip_direction` |
| `lakehouse.gold.daily_location_stats` | Gold | `pulocationid`, `dolocationid`, `date`, `avg_duration`, `avg_fare`, `trip_count`, `total_revenue` |

## 3. Architecture

![Architecture](architectures/medallion-nyc_taxi-spark-iceberg.html)

NYC taxi trip data flows from the bronze table (raw) through Spark batch processing into the silver layer where trips are cleaned and enriched with computed columns (`trip_duration`, `start_hour`, `trip_direction`). From silver, data is aggregated into gold with daily per-location statistics including average duration, average fare, trip counts, and total revenue.

## 4. Notebooks

- **Zeppelin (Scala):** `zeppelin/notebook.zpln` — Sections: Silver Transform (clean + enrich), Gold Aggregation (group by location + date), Verify Both Layers
- **Jupyter (PySpark):** `jupyter/notebook.ipynb` — Same sections; same silver cleaning logic using PySpark, gold aggregation using PySpark `groupBy`

Both languages implement identical silver enrichment and gold aggregation logic across two notebook cells.

## 5. Orchestration

Airflow DAG: `medallion_nyc_taxi` — a scheduled batch DAG.

## 6. Usage

1. Ensure all three Iceberg namespaces exist: `scripts/register_iceberg.py`
2. Populate the bronze layer (if not already done): `airflow dags trigger batch_ingest_nyc_taxi`
3. Open either notebook on the Atlas stack, or trigger the Airflow DAG:
     ```bash
     airflow dags trigger medallion_nyc_taxi
     ```
4. Verify:
     ```bash
     spark-sql -e "SELECT COUNT(*) FROM lakehouse.silver.nyc_taxi_trips"
     spark-sql -e "SELECT * FROM lakehouse.gold.daily_location_stats LIMIT 10"
     ```

## 7. Dependencies

- **Dataset:** NYC taxi trip data (via `lakehouse.bronze.nyc_taxi_trips` populated by batch_ingest)
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other:** None

## 8. Known Issues & Caveats

Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4. All three namespaces (`bronze`, `silver`, `gold`) must exist; run `scripts/register_iceberg.py` first. Depends on the batch_ingest scenario producing the bronze table first.

## See Also

- [Related: batch_ingest-nyc_taxi-spark-iceberg](./batch_ingest-nyc_taxi-spark-iceberg.md) — Produces the bronze source table
- [Related: data_quality-nyc_taxi-spark-iceberg](./data_quality-nyc_taxi-spark-iceberg.md) — Quality checks on ingested data
- [Production Spark app: nyc-taxi-medallion](../spark-apps/nyc-taxi-medallion.md) — Phase-3a JAR productionizes this scenario for Airflow
- [Datasets](../datasets.md)
- [Lakehouse Architecture](../lakehouse.md)
