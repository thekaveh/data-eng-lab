# nyc-taxi-medallion

A Maven Scala Spark app: productionizes the medallion transform pipeline for NYC taxi data.
Bronze Parquet → Silver (deduplicated) → Gold (daily aggregates) in `lakehouse.*` Iceberg tables.
Built by Jenkins, run by Airflow (`spark-submit`).

## 1. Scenario summary
Implements the bronze→silver→gold medallion architecture for NYC taxi trips. Silver deduplicates
on `(tpep_pickup_datetime, tpep_dropoff_datetime)`; Gold aggregates daily trip counts and average
fares per `trip_date`.

## 2. Why this exists
Productionizes the medallion logic prototyped in the `scenarios/medallion-nyc_taxi-spark-iceberg`
notebook into a CI-built, CD-deployed Spark application with scalatest coverage.

## 3. What's in the app
Pure transforms (`transforms/MedallionTransforms` — `silver` + `gold`), scalatest coverage
(`MedallionTransformsSpec`), a `Jenkinsfile` for CI builds, and an Airflow `dag.py` for scheduling.

## 4. How to run
`mvn -q -B test` (unit tests); `mvn -q -B package` (shaded JAR);
Jenkins publishes the JAR to MinIO, Airflow submits it via `spark-submit`.

## 5. Data & dependencies
Spark 4 (`provided`), scalatest 3.2.19. Runtime: Atlas Spark + Iceberg catalog.
Input: `lakehouse.bronze.nyc_taxi_trips`; Outputs: `lakehouse.silver.nyc_taxi_trips`,
`lakehouse.gold.nyc_taxi_daily`.

## 6. Known issues & caveats
The Jenkins/Airflow path is live-gated on Atlas A5/A6.
The live loop additionally requires: a preconfigured `minio` mc alias and a `jars` bucket (A5),
and S3A credentials available on the Spark cluster (A6).
