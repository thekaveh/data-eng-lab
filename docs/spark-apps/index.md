# 7.1. Overview

This directory documents the production Spark applications in the `data-eng-lab` lakehouse. Each application is a Maven-built Scala Spark project with a Jenkins pipeline for build, test, package, and publication, plus an Airflow TaskFlow DAG that uses `SparkSubmitHook` and Atlas's REST-confirmation helper.

The data products form a sequence: `nyc-taxi-etl` creates the Bronze table, and `nyc-taxi-medallion` reads that table to create Silver and Gold tables. The DAGs remain independently scheduled, so operators must ensure the Bronze prerequisite exists before running the medallion DAG.

## Overview

| Application | Description | Source | Target | DAG |
|---|---|---|---|---|
| [nyc-taxi-etl](nyc-taxi-etl.md) | Raw Parquet → Bronze Iceberg with quality filtering | `s3a://landing/nyc_taxi/` | `lakehouse.bronze.nyc_taxi_trips` | `nyc_taxi_etl` |
| [nyc-taxi-medallion](nyc-taxi-medallion.md) | Bronze → Silver dedup → Gold daily aggregation | `lakehouse.bronze.nyc_taxi_trips` | `lakehouse.silver.*`, `lakehouse.gold.*` | `nyc_taxi_medallion` |

## CI/CD Pipeline

Both apps follow the same CI/CD pattern:

1. **CI:** Jenkins runs `mvn test`, then `mvn package`, and publishes the local Maven artifact to the stable MinIO object `s3a://jars/<app>/0.1.0/app.jar`.
2. **CD:** Airflow's `SparkSubmitHook` submits the object in Spark standalone cluster mode with the Iceberg catalog configuration, then confirms the completed driver through Atlas's Spark REST helper.
3. The JAR output is consumed by downstream scenarios or serves as the final medallion-layer output.

The POMs mark Spark as `provided`. The Atlas Spark image supplies the Spark, S3A, and Iceberg runtime used by the cluster driver.

```
GitHub SCM
    │
    ▼
Jenkins CI
  mvn test → mvn package → shaded JAR
    │
    ▼
MinIO (/jars/<app>/0.1.0/app.jar)
    │
    ▼
Airflow (SparkSubmitHook + REST confirmation, cluster mode)
    │
    ▼
Spark Cluster (reads from/sinks to Iceberg tables in S3)
```

## Prerequisites

- Atlas A5 (Jenkins CI) + A6 (Airflow spark-submit CD)
- `mvn` installed locally for testing
- S3A credentials configured on the Spark cluster (for Iceberg catalog access)
- Jenkins MinIO endpoint and Iceberg credentials; each Jenkinsfile creates its `atlas` alias
- Iceberg catalog configuration on the Spark cluster (`spark.sql extensions`, warehouse path, catalog type)
