# data-eng-lab

An Iceberg-lakehouse **data-engineering lab** built on the [Atlas](https://github.com/thekaveh/atlas)
platform. Curated Spark scenarios in **Scala (Zeppelin)** and **PySpark (Jupyter)**, orchestrated with
**Airflow**, plus **Maven Scala Spark** apps built by **Jenkins** and published to **MinIO** — all over
an **Apache Iceberg** lakehouse (REST catalog) on MinIO.

## 1. Overview
See the design spec: [`docs/superpowers/specs/2026-07-02-data-eng-lab-design.md`](docs/superpowers/specs/2026-07-02-data-eng-lab-design.md).

## 2. Repository layout
- `infra/` — Atlas platform (git submodule; never edited)
- `compose/` — the single compose overlay merged into Atlas
- `scenarios/` — flat scenario folders (Zeppelin Scala + Jupyter PySpark [+ DAG]); see [`docs/scenarios.md`](docs/scenarios.md)
- `spark-apps/` — Maven Scala Spark projects (+ Jenkinsfile + DAG); see [`docs/spark-apps.md`](docs/spark-apps.md)
- `datasets/` — dataset registry + downloader
- `scripts/` — bootstrap & tooling
- `tests/` — comprehensive tiered tests (incl. `tests/infra/` preflight)
- `docs/` — design, Atlas contract, per-topic docs

## 3. Quick start
```bash
make setup      # init the Atlas submodule
make up         # launch Atlas data-eng track + bootstrap buckets
make preflight  # prove the stack is up, initialized, and integrated
make down       # tear down
```
See [`docs/datasets.md`](docs/datasets.md) for dataset landing & SCALE configuration.

## 4. Lakehouse + Atlas contract
The lakehouse architecture is documented in [`docs/lakehouse.md`](docs/lakehouse.md).
This repo consumes an enhanced Atlas. The authoritative hand-off of what we expect from Atlas —
delivered vs. outstanding (A7 Trino, A9 Redpanda) plus per-scenario Iceberg/Spark needs — is
[`docs/atlas-expectations.md`](docs/atlas-expectations.md) (origin ledger: [`docs/atlas-enablement.md`](docs/atlas-enablement.md)).

## 5. License
Private / proprietary — see [`LICENSE`](LICENSE).
