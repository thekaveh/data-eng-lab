<p align="center">
  <img src="docs/diagrams/img/data-eng-lab-hero.png" alt="Abstract data-eng-lab lakehouse with Iceberg crystal, medallion layers, and flowing data" width="100%">
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
  <img alt="Redpanda" src="https://img.shields.io/badge/Redpanda-streaming-FF4D5B?logo=apachekafka&logoColor=white">
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

The launcher validates `atlas.consumer.yml`, materializes the Atlas environment, starts the data-engineering track, registers the Iceberg namespaces, and runs the repository preflight. `make datasets` is a separate step that verifies or publishes locked datasets as verified immutable generations; stack startup does not download them. See [Getting started](docs/getting-started.md) for prerequisites, endpoints, and the complete walkthrough.

## 2. Architecture

The landing zone and the three Iceberg medallion layers are distinct storage stages:

```text
s3a://landing/  →  bronze  →  silver  →  gold
raw source data    clean      enriched    aggregated/modelled
```

![data-eng-lab architecture](docs/diagrams/img/overview.png)

Every curated table is an Apache Iceberg table accessed through the Atlas Iceberg REST catalog (`lakehouse`). Spark supplies compute, Trino handles ad-hoc and federated SQL, and Airflow schedules production DAGs. Jenkins builds Maven Scala Spark apps and publishes their JARs to MinIO. Redpanda (Kafka-compatible) backs the event-ingest, windowing, and CDC streaming scenarios. `streaming_ingest-gh_archive-spark-iceberg` uses an incremental file source and requires no Kafka broker.

## 3. Explore the lab

| Destination | What it contains |
|---|---|
| [Scenario catalog](docs/scenarios/index.md) | All 19 end-to-end scenarios, dependencies, execution modes, and source datasets |
| [Execution-mode matrix](docs/scenarios/execution-modes.md) | Five production DAGs, three approved children, seven notebook-only scenarios, and three unscheduled streams |
| [Notebook walkthroughs](docs/notebooks/index.md) | Side-by-side docs for 17 Scala/PySpark pairs and two Trino SQL/client pairs |
| [Spark apps](docs/spark-apps/index.md) | Five CI-built Maven applications published by Jenkins and submitted through Airflow |
| [Datasets](docs/datasets.md) | NYC Taxi, TPC-H, Online Retail, GH Archive, and MovieLens data plus synthetic events |
| [Lakehouse design](docs/lakehouse.md) | Landing, bronze, silver, gold, namespaces, buckets, and catalog behavior |
| [Atlas enablement](docs/atlas-enablement.md) | Accepted consumer configuration and the A1–A9 infrastructure record |
| [Go-live runbook](docs/go-live.md) | Reproducible platform, notebook, DAG, and Spark-application acceptance |

## 4. By the numbers

| Inventory | Count |
|---|---:|
| Paired Zeppelin and Jupyter scenario implementations | 19 |
| Dual-language Scala/PySpark parity pairs | 17 |
| Redpanda-backed Structured Streaming scenarios | 3 |
| Incremental file-source Structured Streaming scenarios | 1 |
| CI-built Maven Spark apps | 5 |
| Curated downloaded datasets | 5 |
| Iceberg medallion layers | 3 |

> New here? Run the [Getting started](docs/getting-started.md) walkthrough, then choose a scenario from the [catalog](docs/scenarios/index.md).
