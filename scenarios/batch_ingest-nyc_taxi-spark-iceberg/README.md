# batch_ingest-nyc_taxi-spark-iceberg

Batch ingestion: read raw NYC taxi Trips Parquet from `s3a://landing/nyc_taxi/*` and write to an Iceberg bronze table. Scala (Zeppelin) and PySpark (Jupyter) notebooks implement the same logic.

## 1. Purpose

This is the first step in the medallion architecture — ingesting raw Parquet data into Iceberg with full history retention and schema enforcement. The bronze layer mirrors source data exactly, preserving all fields as-is and maintaining the original partitioning structure.

## 2. Data Model

### 2.1 Input Source

Source: `s3a://landing/nyc_taxi/*.parquet` (downloaded via `make datasets`).

| Column | Type | Notes |
|---|---|---|
| `VendorID` | double | Vendor identifier |
| `tpep_pickup_datetime` | timestamp | Pickup timestamp |
| `tpep_dropoff_datetime` | timestamp | Dropoff timestamp |
| `passenger_count` | int | Number of passengers |
| `trip_distance` | double | Trip distance in miles |
| `RatecodeID` | double | Rate code |
| `store_and_fwd_flag` | string | Store and forward flag |
| `PULocationID` | int | Pickup location ID |
| `DOLocationID` | int | Dropoff location ID |
| `payment_type` | double | Payment type |
| `fare_amount` | double | Fare amount |
| `extra` | double | Extra charges |
| `mta_tax` | double | MTA tax |
| `tip_amount` | double | Tip amount |
| `tolls_amount` | double | Tolls amount |
| `improvement_surcharge` | double | Improvement surcharge |
| `total_amount` | double | Total amount |
| `congestion_surcharge` | double | Congestion surcharge |

### 2.2 Output Tables

| Table | Layer | Key Columns |
|---|---|---|
| `lakehouse.bronze.nyc_taxi_trips` | Bronze | All columns as in source |

## 3. Architecture

![Architecture](../../docs/scenarios/architectures/batch_ingest-nyc_taxi-spark-iceberg.html)

Raw Parquet trip data flows from the S3 landing zone through Spark batch processing directly into an Iceberg bronze table in the `lakehouse.bronze` namespace, preserving the original schema and all fields without transformation.

## 4. Notebooks

- **Zeppelin (Scala):** `zeppelin/notebook.zpln` — Sections: Overview, Read Raw Parquet, Write to Iceberg, Verify
- **Jupyter (PySpark):** `jupyter/notebook.ipynb` — Same sections; same batch ingest logic using PySpark DataFrame reader and writer

Both languages implement identical ingestion logic with source read, Iceberg write, and verification sections.

## 5. Orchestration

Airflow DAG: `batch_ingest_nyc_taxi` — a scheduled batch DAG.

## 6. Usage

1. Ensure the `bronze` Iceberg namespace exists: `scripts/register_iceberg.py`
2. Populate the landing zone: `make datasets`
3. Open either notebook on the Atlas stack, or trigger the Airflow DAG:
     ```bash
     airflow dags trigger batch_ingest_nyc_taxi
     ```
4. Verify:
     ```bash
     spark-sql -e "SELECT COUNT(*) FROM lakehouse.bronze.nyc_taxi_trips"
     ```

## 7. Dependencies

- **Dataset:** NYC Taxi Trips Parquet from `s3a://landing/nyc_taxi/`
- **Atlas services:** A1-A4 (Spark, Iceberg, S3 catalog, lakehouse catalog)
- **Other:** None

## 8. Known Issues & Caveats

Notebook execution and Scala/PySpark parity are live-gated on Atlas A1-A4. The `bronze` namespace must exist; run `scripts/register_iceberg.py` first. At scale, the inline seed can be replaced by the registered CSV dataset. Drop the target table first for a clean demo if re-running.

## See Also

- [Related: table_maintenance-nyc_taxi-spark-iceberg](../table_maintenance-nyc_taxi-spark-iceberg/README.md) — Table maintenance patterns
- [Related: data_quality-nyc_taxi-spark-iceberg](../data_quality-nyc_taxi-spark-iceberg/README.md) — Quality checks on ingested data
- [Related: medallion-nyc_taxi-spark-iceberg](../medallion-nyc_taxi-spark-iceberg/README.md) — Medallion transforms downstream
- [Related: time_travel-nyc_taxi-spark-iceberg](../time_travel-nyc_taxi-spark-iceberg/README.md) — Iceberg time travel on ingested tables
- [Production Spark app: nyc-taxi-etl](../../docs/spark-apps/nyc-taxi-etl.md) — Phase-3a JAR productionizes this scenario for Airflow
- [Datasets](../../docs/datasets.md)
- [Lakehouse Architecture](../../docs/lakehouse.md)
