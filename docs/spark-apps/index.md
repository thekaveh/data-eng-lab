# Spark Applications

This directory documents the production Spark applications in the `data-eng-lab` lakehouse. Each application is a Maven-built Scala Spark project with a Jenkinsfile for CI (build, test, package shaded JAR) and an Airflow DAG for CD (SparkSubmitOperator cluster-mode deployment). Together, these apps productionize the notebook prototypes originally authored as Spark-Iceberg scenarios.

The applications form a sequential pipeline: raw Parquet data is first ingested and quality-filtered into the bronze layer (`nyc-taxi-etl`), then deduplicated and aggregated across silver and gold layers (`nyc-taxi-medallion`). The shaded JARs produced by Jenkins include all runtime dependencies (Iceberg bindings bundled, Spark marked `provided`) and are published to MinIO for Airflow's `SparkSubmitOperator` to consume.

## Overview

| Application | Description | Source | Target | DAG |
|---|---|---|---|---|
| [nyc-taxi-etl](nyc-taxi-etl.md) | Raw Parquet → Bronze Iceberg with quality filtering | `s3a://landing/nyc_taxi/` | `lakehouse.bronze.nyc_taxi_trips` | `nyc_taxi_etl` |
| [nyc-taxi-medallion](nyc-taxi-medallion.md) | Bronze → Silver dedup → Gold daily aggregation | `lakehouse.bronze.nyc_taxi_trips` | `lakehouse.silver.*`, `lakehouse.gold.*` | `nyc_taxi_medallion` |

## CI/CD Pipeline

Both apps follow the same CI/CD pattern:

1. **CI:** Jenkins clones the repo → `mvn test` (ScalaTest) → `mvn package` (Maven Shade plugin) → produces a shaded JAR → publishes to `s3a://jars/<app>/0.1.0/<app>.jar` in MinIO.
2. **CD:** Airflow's `SparkSubmitOperator` (cluster deploy mode) downloads the JAR from MinIO and submits it to the Spark cluster with Iceberg catalog configuration.
3. The JAR output is consumed by downstream scenarios or serves as the final medallion-layer output.

```
GitHub SCM
    │
    ▼
Jenkins CI
  mvn test → mvn package → shaded JAR
    │
    ▼
MinIO (/jars/<app>/0.1.0/<app>.jar)
    │
    ▼
Airflow (SparkSubmitOperator, cluster mode)
    │
    ▼
Spark Cluster (reads from/sinks to Iceberg tables in S3)
```

## Prerequisites

- Atlas A5 (Jenkins CI) + A6 (Airflow spark-submit CD)
- `mvn` installed locally for testing
- S3A credentials configured on the Spark cluster (for Iceberg catalog access)
- MinIO `mc` alias and `jars` bucket (A5)
- Iceberg catalog configuration on the Spark cluster (`spark.sql extensions`, warehouse path, catalog type)
