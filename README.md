# data-eng-lab

An Apache Iceberg lakehouse data engineering lab built on the [Atlas](https://github.com/thekaveh/atlas) platform. The repository contains 19 Spark scenarios, 2 CI-built Maven Scala applications, Trino SQL BI queries, Redpanda streaming, and a full medallion (bronze → silver → gold) architecture — all orchestrated via Docker Compose.

📖 [Full Documentation Site](https://thekaveh.github.io/data-eng-lab/)

---

## Overview

`data-eng-lab` is a curated collection of data-engineering scenarios and production-grade Spark applications running over a full Iceberg lakehouse (REST catalog + MinIO). It is built on the Atlas platform — a Docker Compose-based stack providing Spark, Zeppelin, Jupyter, Airflow, Jenkins, Trino, and Redpanda — consumed as a pinned submodule via `infra/` (never edited directly). Every scenario is implemented twice for parity: Scala in Zeppelin and PySpark in Jupyter. Maven Scala Spark apps are built by Jenkins, published as shaded JARs to MinIO, and invoked by Airflow DAGs — closing the build-to-orchestration loop on a true medallion bronze → silver → gold pipeline.

## By the Numbers

| Metric | Value |
|---|---|
| Spark scenarios | 19 (14 batch, 4 streaming, 1 hybrid) |
| Spark applications | 2 (Maven Scala, CI/CD automated) |
| Curated datasets | 5 (NYC Taxi, TPC-H, MovieLens, Online Retail, GitHub Archive) |
| Atlas enablement items | 9 (A1–A9, all delivered) |
| Query engines | 2 (Spark SQL, Trino SQL) |

## Quick Start

```bash
git clone https://github.com/thekaveh/data-eng-lab.git
cd data-eng-lab
make setup          # initialize Atlas submodule
make datasets       # download datasets
make up             # launch all services
make preflight      # verify connectivity
```

See [Getting Started](docs/getting-started.md) for the full guide with prerequisites, troubleshooting, and next steps.

## Scenarios

| Name | Dataset | Mode | Medallion Layers | Airflow DAG | Description |
|---|---|---|---|---|---|
| batch_ingest | NYC Taxi | Batch | Bronze | Yes | Raw Parquet → Iceberg Bronze layer |
| medallion | NYC Taxi | Batch | Bronze → Silver → Gold | Yes | Full multi-layer transformation pipeline |
| streaming_ingest (events) | Events | Streaming | Bronze | Yes | Structured Streaming from Redpanda topic |
| streaming_ingest (gh_archive) | GitHub Archive | Streaming | Bronze | No | File-source Structured Streaming with checkpoints |
| streaming_windows | Events | Streaming | Silver | No | Windowed aggregation and watermark on Kafka stream |
| cdc_streaming | Online Retail | Streaming | Bronze → Silver | No | Streaming CDC upserts via `foreachBatch` and `MERGE INTO` |
| data_quality | NYC Taxi | Batch | Bronze + quarantine | No | Validation, quarantine table, and metrics |
| schema_evolution | GitHub Archive | Batch | Varied | No | Add/rename columns, read old + new data together |
| time_travel | NYC Taxi | Batch | Varied | Yes | Snapshots, `VERSION AS OF`, rollback, branch/tag |
| table_maintenance | NYC Taxi | Batch | Varied | No | Compaction, `expire_snapshots`, `remove_orphan_files` |
| star_schema | TPC-H | Batch | Bronze → Silver → Gold | No | Fact/dimension gold marts and dimensional modeling |
| bi_query | TPC-H | Batch (Trino) | Read | No | Multi-engine BI read via Trino over Spark-written gold marts |
| federated_query | NYC Taxi | Batch (Trino) | Read | Yes | Federated query across Bronze and Gold tables via Trino |
| join_optimization | TPC-H | Batch | Varied | No | Broadcast vs sort-merge join and adaptive query execution |
| feature_engineering | MovieLens | Batch | Feature marts | No | ML feature marts (bridges to ml-lab) |
| scd2 | Online Retail | Batch | Silver | No | Slowly Changing Dimension Type 2 (effective_from/to, is_current) |
| incremental_upsert | Online Retail | Batch | Silver | No | CDC/upsert pattern with `MERGE INTO` |
| json_flatten | GitHub Archive | Batch | Bronze → Silver | No | Nested JSON transformation into typed columns |
| sessionization | GitHub Archive | Batch | Varied | No | Window functions and gap-based sessions |

See [Scenario Catalog](docs/scenarios.md) for the full index with architecture diagrams.

### Scenarios by Category

**Batch Ingestion** — `batch_ingest`

**Medallion Pipeline** — `medallion`

**Data Quality** — `data_quality`

**Schema & Maintenance** — `schema_evolution`, `time_travel`, `table_maintenance`

**Streaming** — `streaming_ingest` (events + gh_archive), `streaming_windows`, `cdc_streaming`

**BI & Queries** — `federated_query`, `bi_query`

**Join Optimization** — `join_optimization`

**Dimensional Modeling** — `star_schema`

**Feature Engineering** — `feature_engineering`

**SCD** — `scd2`

**JSON Processing** — `json_flatten`

**Session Analysis** — `sessionization`

## Spark Applications

| Application | Description |
|---|---|
| [nyc-taxi-etl](spark-apps/nyc-taxi-etl/) | Raw Parquet → cleaned Bronze Iceberg |
| [nyc-taxi-medallion](spark-apps/nyc-taxi-medallion/) | Bronze → Silver → Gold medallion pipeline |

Built by Jenkins CI, submitted via Airflow DAG. See [Spark Apps](docs/spark-apps/index.md) for details.

## Repository Structure

```
data-eng-lab/
├── scenarios/           # 19 scenario folders — Zeppelin Scala + Jupyter PySpark (+ DAG)
├── spark-apps/          # Maven Scala Spark projects + Jenkinsfile + Airflow DAG
├── datasets/            # dataset registry (registry.yaml) + downloader script
├── lakehouse/           # Iceberg catalog/table configuration
├── tests/               # tiered tests: unit, static, network, infra preflight
├── docs/                # MkDocs Material site source (published to GitHub Pages)
├── infra/               # Atlas platform submodule (pinned; never edited)
├── compose/             # single Docker-compose overlay for Atlas
├── scripts/             # bootstrap, start-all.sh, stop-all.sh, dataset downloader, verifier
├── Makefile             # canonical entry points (setup / up / down / datasets / test / lint)
└── pyproject.toml       # uv project — all dev deps in [dependency-groups].dev
```

## License

This project uses a proprietary license — see [`LICENSE`](LICENSE) for terms. The datasets used in scenarios have their own open licenses as declared in [`datasets/registry.yaml`](datasets/registry.yaml).

---

*Maintained by `data-eng-lab`. Questions or issues → open a GitHub issue.*
