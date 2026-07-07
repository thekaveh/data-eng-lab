<!-- AUTO-GENERATED — do not edit; run scripts/build_docs.py -->
# NYC Taxi Medallion Pipeline

Productionizes the medallion transform pattern for NYC taxi trip data: bronze Iceberg → silver (deduplicated) → gold (daily aggregation on trip counts and average fares). Built by Jenkins CI, orchestrated by Airflow CD.

## 1. Architecture

```
lakehouse.bronze.nyc_taxi_trips
           │
           │  (Iceberg table, Spark reads)
           ▼
    ┌─────────────┐
    │   GitHub     │  (Git SCM trigger)
    └──────┬──────┘
           │
           ▼
    ┌─────────────┐      ┌──────────┐
    │  Jenkins CI  │─────▶│  MinIO   │
    │ mvn test     │      │  jars/   │
    │ mvn package  │      │ app.jar  │
    └──────┬──────┘      └────┬─────┘
           │                   │
           ▼                   ▼
    ┌─────────────────────────────┐
    │        Airflow DAG           │
    │   SparkSubmitOperator        │
    └──────────────┬──────────────┘
                   │
                   ▼
    ┌──────────────────────────────────────────┐
    │           Spark Cluster                   │
    │                                           │
    │   read bronze → MedallionTransforms       │
    │     silver: dedup on (pickup, dropoff)    │
    │     gold:   aggregate by trip_date        │
    │           → write silver + gold           │
    └────────────────────────┬─────────────────┘
             ┌────────────────┴────────────────┐
             ▼                                  ▼
    ┌──────────────────┐          ┌──────────────────────┐
    │ lakehouse.silver  │          │ lakehouse.gold        │
    │ .nyc_taxi_trips   │          │ .nyc_taxi_daily        │
    │ (deduplicated)    │          │ (trip counts, avg fare)│
    └──────────────────┘          └──────────────────────┘
```

![Architecture](../architectures/nyc-taxi-medallion.svg)

- **Iceberg bronze → Jenkins:** SCM poll triggers the pipeline; Spark reads from the bronze layer.
- **Jenkins CI:** runs `mvn test` (ScalaTest) then `mvn package`, producing a shaded JAR.
- **MinIO:** JAR is published to `s3a://jars/nyc-taxi-medallion/0.1.0/nyc-taxi-medallion.jar`.
- **Airflow:** `SparkSubmitOperator` submits the JAR to the Spark cluster in cluster mode.
- **Spark Cluster:** reads from `lakehouse.bronze.nyc_taxi_trips`, applies `MedallionTransforms.silver()` for deduplication and `MedallionTransforms.gold()` for daily aggregation, writes both tables.

## 2. Project Structure

- **Language:** Scala (2.12)
- **Build tool:** Maven (3.8+)
- **Testing:** ScalaTest 3.2.19 — `MedallionTransformsSpec` with parameterised property tests
- **Transform source:** `src/main/scala/transforms/MedallionTransforms.scala`
- **Entrypoint:** `src/main/scala/MedallionMedallion.scala` (argument-driven: accepts `--bronze` / `--silver` / `--gold`)
- **CI/CD:** `Jenkinsfile`, `src/main/scala/dag.py`

## 3. Transform Logic

The `MedallionTransforms` object defines two public methods:

### `silver(df): DataFrame` — Bronze → Silver (deduplication)

1. **Input:** `lakehouse.bronze.nyc_taxi_trips` with columns: `id`, `type`, `actor_login`, `repo_name`, `created_at`, `trip_date`, etc.
2. **Operation:** `df.dropDuplicates("tpep_pickup_datetime", "tpep_dropoff_datetime")` — removes rows sharing the same pickup and dropoff datetime pair, keeping the first occurrence.
3. **Output:** Deduplicated DataFrame with the same schema, ready for the silver layer.

### `gold(silverDf): DataFrame` — Silver → Gold (daily aggregation)

1. **Input:** Deduplicated silver DataFrame.
2. **Operations:**
   - Derive `trip_date = to_date(tpep_pickup_datetime)` if not already present.
   - `groupBy("trip_date").agg(
       count("id").alias("trip_count"),
       avg("fare_amount").alias("avg_fare")
     )`
3. **Output:** Daily summary table with columns: `trip_date`, `trip_count`, `avg_fare`.

Spark concepts used: `dropDuplicates`, `groupBy().agg()`, `avg`, `count`, `to_date`, `writeTo(...).overwrite()` for idempotent gold writes.

## 4. Build & Test

```bash
# Run unit tests
mvn -q -B -f spark-apps/nyc-taxi-medallion/pom.xml test

# Build the shaded JAR
mvn -q -B -f spark-apps/nyc-taxi-medallion/pom.xml package
```

## 5. Run with Airflow

The DAG (`nyc_taxi_medallion`) uses `SparkSubmitOperator` configured with:

- **application:** `s3a://jars/nyc-taxi-medallion/0.1.0/nyc-taxi-medallion.jar`
- **deploy-mode:** `cluster` (Spark runs on cluster YARN/K8s, not driver)
- **conf:** Iceberg SPARK config (`spark.sql.extensions=org.apache.iceberg.spark.IcebergSparkSessionExtensions`, catalog config with `warehouse=...`, `io-impl=...`)
- **jars:** the MinIO-published JAR
- **dependencies:** no external PyPI packages; the JAR ships its shaded dependencies (Spark 4 as `provided`, Iceberg runtime bundled)
- **depends_on:** upstream `nyc_taxi_etl` DAG (Airflow task group dependency)

Spark reads `lakehouse.bronze.nyc_taxi_trips` and writes to:
- `lakehouse.silver.nyc_taxi_trips` (deduplicated)
- `lakehouse.gold.nyc_taxi_daily` (daily aggregation, `overwrite` mode for idempotency)

## 6. Prerequisites

- Atlas A5 (Jenkins CI pipeline)
- Atlas A6 (Airflow SparkSubmitOperator)
- JAR published to `s3a://jars/nyc-taxi-medallion/0.1.0/nyc-taxi-medallion.jar`
- Preconfigured `minio` `mc` alias and `jars` bucket (A5)
- S3A credentials available on the Spark cluster (A6)
- `lakehouse.bronze.nyc_taxi_trips` populated by upstream `nyc-taxi-etl` DAG
- Iceberg catalog configured on the Spark cluster

## 7. Data Flow

```
lakehouse.bronze.nyc_taxi_trips
       │
       │  read (Iceberg table, Spark DataFrame)
       ▼
  MedallionTransforms.silver()
       │  dropDuplicates(pickup_datetime, dropoff_datetime)
       ▼
  lakehouse.silver.nyc_taxi_trips  (deduplicated Bronze data)
       │
       │  read (silver layer)
       ▼
  MedallionTransforms.gold()
       │  groupBy(trip_date)
       │  → trip_count = count(*)
       │  → avg_fare = avg(fare_amount)
       ▼
  lakehouse.gold.nyc_taxi_daily  (daily aggregation, overwrite)
  [trip_date | trip_count | avg_fare]
```

## 8. See Also

- [Spark apps overview](../index/README.md)
- [nyc-taxi-etl](../nyc-taxi-etl/README.md)
- [Related scenario: medallion-nyc_taxi-spark-iceberg](../../scenarios/medallion-nyc_taxi-spark-iceberg/README.md) — Notebook prototype of this app
- [Related scenario: batch_ingest-nyc_taxi-spark-iceberg](../../scenarios/batch_ingest-nyc_taxi-spark-iceberg/README.md) — Populates the bronze table this app reads from
- [Lakehouse Architecture](../../README.md#lakehouse-architecture)
- [Datasets](../../README.md#datasets)
