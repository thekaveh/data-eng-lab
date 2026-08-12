# 7.1. Overview

This directory documents the production Spark applications in the `data-eng-lab` lakehouse. Each application is a Maven-built Scala Spark project with a Jenkins pipeline for build, test, package, and publication, plus an Airflow DAG that uses an operator-owned `SparkSubmitOperator` subclass and Atlas's `RestConfirmingSparkHook` adapter.

The NYC Taxi applications form a sequence from Bronze to Silver and Gold. The independent TPC-H and MovieLens applications each build two Gold tables from one complete verified publication and record equal provenance on both outputs.

## 1. Overview

| Application | Description | Source | Target | DAG |
|---|---|---|---|---|
| [nyc-taxi-etl](nyc-taxi-etl.md) | Raw Parquet → Bronze Iceberg with quality filtering | resolver-verified immutable NYC Taxi generation | `lakehouse.bronze.nyc_taxi_trips` | `nyc_taxi_etl` |
| [nyc-taxi-medallion](nyc-taxi-medallion.md) | Bronze → Silver dedup → Gold daily aggregation | `lakehouse.bronze.nyc_taxi_trips` | `lakehouse.silver.*`, `lakehouse.gold.*` | `nyc_taxi_medallion` |
| [tpch-star-schema](tpch-star-schema.md) | Verified TPC-H → customer dimension and order fact | immutable TPC-H generation | `lakehouse.gold.dim_customer`, `lakehouse.gold.fct_orders` | `tpch_star_schema` |
| [movielens-feature-pipeline](movielens-feature-pipeline.md) | Verified MovieLens ratings → user and movie aggregates | immutable MovieLens generation | `lakehouse.gold.ml_user_features`, `lakehouse.gold.ml_movie_features` | `movielens_feature_pipeline` |

## 2. CI/CD Pipeline

All four apps follow the same CI/CD pattern:

1. **CI:** Jenkins runs `mvn test`, then `mvn package`, and publishes the local Maven artifact to the stable MinIO object `s3a://jars/<app>/0.1.0/app.jar`.
2. **CD:** Airflow's `SparkSubmitOperator` submits the object in Spark standalone cluster mode with the Iceberg catalog configuration; its hook is wrapped by Atlas's adapter to confirm the completed driver through the Spark REST endpoint.
3. The JAR output is consumed by downstream scenarios or serves as the final medallion-layer output.

The POMs mark Spark as `provided`. The Atlas Spark image supplies the Spark, S3A, and Iceberg runtime used by the cluster driver.

```text
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
Airflow (SparkSubmitOperator + REST-confirming hook, cluster mode)
    │
    ▼
Spark Cluster (reads from/sinks to Iceberg tables in S3)
```

## 3. Prerequisites

- Atlas A5 (Jenkins CI) + A6 (Airflow spark-submit CD)
- `mvn` installed locally for testing
- S3A credentials configured on the Spark cluster (for Iceberg catalog access)
- Jenkins MinIO endpoint and Iceberg credentials; each Jenkinsfile creates its `atlas` alias
- Iceberg catalog configuration on the Spark cluster (`spark.sql extensions`, warehouse path, catalog type)
