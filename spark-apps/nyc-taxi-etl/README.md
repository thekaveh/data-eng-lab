# nyc-taxi-etl

A Maven Scala Spark app: raw NYC-taxi Parquet in `s3a://landing/nyc_taxi/` → cleaned Iceberg
`lakehouse.bronze.nyc_taxi_trips`. Built by Jenkins, run by Airflow (`spark-submit`).

## 1. Scenario summary
## 2. Why this exists
## 3. What's in the app
Pure transforms (`transforms/TaxiTransforms`), an argument-driven entrypoint (`NycTaxiEtl`), scalatest,
a `Jenkinsfile`, and an Airflow `dag.py`.
## 4. How to run
`mvn -q -B test` (unit); `mvn -q -B package` (shaded JAR); Jenkins publishes it, Airflow submits it.
## 5. Data & dependencies
Spark 4 (`provided`), scalatest. Runtime: Atlas Spark + Iceberg.
## 6. Known issues & caveats
The Jenkins/Airflow path is live-gated on Atlas A5/A6.
The live loop additionally requires: a preconfigured `minio` mc alias and a `jars` bucket (A5), and S3A credentials available on the Spark cluster (A6).
