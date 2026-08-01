<p align="center">
  <img src="diagrams/img/data-eng-lab-hero.png" alt="Abstract data-eng-lab lakehouse with Iceberg crystal, medallion layers, and flowing data" width="100%">
</p>

<h1 align="center">data-eng-lab</h1>

<p align="center">
  <strong>An Iceberg-lakehouse data-engineering lab built on the <a href="https://github.com/thekaveh/atlas">Atlas</a> platform.</strong>
</p>

<p align="center">
  Build, orchestrate, stream, and query production-shaped lakehouse pipelines from paired notebooks and deployable Spark applications.
</p>

<p align="center">
  <img alt="Atlas" src="https://img.shields.io/badge/Atlas-infrastructure-2563EB?logo=git&logoColor=white">
  <img alt="Docker Compose" src="https://img.shields.io/badge/Docker%20Compose-runtime-2496ED?logo=docker&logoColor=white">
</p>

<p align="center">
  <img alt="Apache Spark" src="https://img.shields.io/badge/Apache%20Spark-compute-E25A1C?logo=apachespark&logoColor=white">
  <img alt="Apache Iceberg" src="https://img.shields.io/badge/Apache%20Iceberg-tables-4F46E5?logo=apache&logoColor=white">
  <img alt="MinIO" src="https://img.shields.io/badge/MinIO-object%20storage-C72E49?logo=minio&logoColor=white">
  <img alt="Trino" src="https://img.shields.io/badge/Trino-SQL-DD00A1?logo=trino&logoColor=white">
  <img alt="Redpanda" src="https://img.shields.io/badge/Redpanda-streaming-FF4D5B?logo=redpanda&logoColor=white">
</p>

<p align="center">
  <img alt="Apache Airflow" src="https://img.shields.io/badge/Apache%20Airflow-orchestration-017CEE?logo=apacheairflow&logoColor=white">
  <img alt="Jenkins" src="https://img.shields.io/badge/Jenkins-CI-D24939?logo=jenkins&logoColor=white">
  <img alt="Maven" src="https://img.shields.io/badge/Maven-builds-C71A36?logo=apachemaven&logoColor=white">
  <img alt="Jupyter" src="https://img.shields.io/badge/Jupyter-notebooks-F37626?logo=jupyter&logoColor=white">
  <img alt="Zeppelin" src="https://img.shields.io/badge/Zeppelin-notebooks-FBBF24?logo=apache&logoColor=white">
</p>

`data-eng-lab` consumes Atlas as its pinned `infra/` git submodule through `atlas.consumer.yml`, so `make up` launches the default development profile as the **Data Engineering** workspace. It pairs 19 Zeppelin and Jupyter scenario notebooks—17 Scala/PySpark implementations plus two Trino client pairs—with Iceberg on MinIO, Airflow, Jenkins-built Spark apps, Trino, and Redpanda for three broker-backed streams.

## 1. Quick start

```bash
git submodule update --init --recursive infra
uv sync --all-groups
make up
make datasets
make verify
```

The launcher validates `atlas.consumer.yml`, materializes the Atlas environment, starts the data-engineering track, registers the Iceberg namespaces, and runs the repository preflight. See [Getting started](getting-started.md) for prerequisites, endpoints, and the complete walkthrough.

## 2. Architecture

The landing zone and the three Iceberg medallion layers are distinct storage stages:

```text
s3a://landing/  →  bronze  →  silver  →  gold
raw source data    clean      enriched    aggregated/modelled
```

![data-eng-lab architecture](diagrams/img/overview.png)

Every curated table is an Apache Iceberg table accessed through the Atlas Iceberg REST catalog (`lakehouse`). Spark supplies compute, Trino handles ad-hoc and federated SQL, and Airflow schedules production DAGs. Jenkins builds Maven Scala Spark apps and publishes their JARs to MinIO. Redpanda (Kafka-compatible) backs the event-ingest, windowing, and CDC streaming scenarios. `streaming_ingest-gh_archive-spark-iceberg` uses an incremental file source and requires no Kafka broker.

## 3. Explore the lab

| Destination | What it contains |
|---|---|
| [Scenario catalog](scenarios/index.md) | All 19 end-to-end scenarios, dependencies, execution modes, and source datasets |
| [Notebook walkthroughs](notebooks/index.md) | Side-by-side docs for 17 Scala/PySpark pairs and two Trino SQL/client pairs |
| [Spark apps](spark-apps/index.md) | Two CI-built Maven applications published by Jenkins and submitted through Airflow |
| [Datasets](datasets.md) | NYC Taxi, TPC-H, Online Retail, GH Archive, and MovieLens data plus synthetic events |
| [Lakehouse design](lakehouse.md) | Landing, bronze, silver, gold, namespaces, buckets, and catalog behavior |
| [Atlas enablement](atlas-enablement.md) | Accepted consumer configuration and the A1–A9 infrastructure record |
| [Go-live runbook](go-live.md) | Reproducible platform, notebook, DAG, and Spark-application acceptance |

## 4. Scenario catalog

| Scenario | Engine | Layer | Dataset/source |
|---|---|---|---|
| [batch_ingest-nyc_taxi-spark-iceberg](scenarios/batch_ingest-nyc_taxi-spark-iceberg.md) | Spark | Bronze | NYC Taxi |
| [medallion-nyc_taxi-spark-iceberg](scenarios/medallion-nyc_taxi-spark-iceberg.md) | Spark | Bronze → Silver → Gold | NYC Taxi |
| [data_quality-nyc_taxi-spark-iceberg](scenarios/data_quality-nyc_taxi-spark-iceberg.md) | Spark | Silver | NYC Taxi |
| [schema_evolution-gh_archive-spark-iceberg](scenarios/schema_evolution-gh_archive-spark-iceberg.md) | Spark | Silver | GH Archive |
| [time_travel-nyc_taxi-spark-iceberg](scenarios/time_travel-nyc_taxi-spark-iceberg.md) | Spark | Silver | NYC Taxi |
| [table_maintenance-nyc_taxi-spark-iceberg](scenarios/table_maintenance-nyc_taxi-spark-iceberg.md) | Spark | Silver | NYC Taxi |
| [streaming_ingest-events-spark-iceberg](scenarios/streaming_ingest-events-spark-iceberg.md) | Spark stream | Bronze | Redpanda events |
| [streaming_ingest-gh_archive-spark-iceberg](scenarios/streaming_ingest-gh_archive-spark-iceberg.md) | Spark stream | Bronze | GH Archive files |
| [streaming_windows-events-spark-iceberg](scenarios/streaming_windows-events-spark-iceberg.md) | Spark stream | Silver | Redpanda events |
| [cdc_streaming-online_retail-spark-iceberg](scenarios/cdc_streaming-online_retail-spark-iceberg.md) | Spark stream | Silver | Redpanda CDC |
| [federated_query-nyc_taxi-trino-iceberg](scenarios/federated_query-nyc_taxi-trino-iceberg.md) | Trino | Gold | NYC Taxi |
| [bi_query-tpch-trino-iceberg](scenarios/bi_query-tpch-trino-iceberg.md) | Trino | Gold | TPC-H |
| [join_optimization-tpch-spark-iceberg](scenarios/join_optimization-tpch-spark-iceberg.md) | Spark | Gold | TPC-H |
| [star_schema-tpch-spark-iceberg](scenarios/star_schema-tpch-spark-iceberg.md) | Spark | Gold | TPC-H |
| [feature_engineering-movielens-spark-iceberg](scenarios/feature_engineering-movielens-spark-iceberg.md) | Spark | Gold | MovieLens |
| [scd2-online_retail-spark-iceberg](scenarios/scd2-online_retail-spark-iceberg.md) | Spark | Silver | Online Retail |
| [json_flatten-gh_archive-spark-iceberg](scenarios/json_flatten-gh_archive-spark-iceberg.md) | Spark | Silver | GH Archive |
| [sessionization-gh_archive-spark-iceberg](scenarios/sessionization-gh_archive-spark-iceberg.md) | Spark | Silver | GH Archive |
| [incremental_upsert-online_retail-spark-iceberg](scenarios/incremental_upsert-online_retail-spark-iceberg.md) | Spark | Silver | Online Retail |

## 5. By the numbers

| Inventory | Count |
|---|---:|
| Paired Zeppelin and Jupyter scenario implementations | 19 |
| Dual-language Scala/PySpark parity pairs | 17 |
| Redpanda-backed Structured Streaming scenarios | 3 |
| Incremental file-source Structured Streaming scenarios | 1 |
| CI-built Maven Spark apps | 2 |
| Curated downloaded datasets | 5 |
| Iceberg medallion layers | 3 |

> New here? Run the [Getting started](getting-started.md) walkthrough, then choose a scenario from the [catalog](scenarios/index.md).
